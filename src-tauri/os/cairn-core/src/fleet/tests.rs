use super::*;
use crate::storage::RowExt;
use cairn_common::executor_protocol::{
    AllocatorState, LivenessRefreshBlocked, LivenessRefreshPhase, RemoteConnectionPhase,
    LIVENESS_REFRESH_BOUND_MS,
};

struct TestListenerRotationControl {
    regenerate_at: AtomicU64,
    status: ListenerRotationStatus,
    casualties: Vec<String>,
    endpoints: Mutex<Vec<String>>,
}

impl ListenerRotationControl for TestListenerRotationControl {
    fn pending(&self) -> Result<Option<ListenerRotationStatus>, String> {
        Ok(Some(self.status.clone()))
    }

    fn regenerate(&self, started_at_unix_ms: u64) -> Result<ListenerRotationStatus, String> {
        self.regenerate_at
            .store(started_at_unix_ms, Ordering::SeqCst);
        Ok(self.status.clone())
    }

    fn force_cutover(&self) -> Result<Vec<String>, String> {
        Ok(self.casualties.clone())
    }

    fn update_endpoints(&self, endpoints: Vec<String>) -> Result<(), String> {
        *self.endpoints.lock().unwrap() = endpoints;
        Ok(())
    }

    fn hold_for_connection(&self, _executor_id: &str, _display_name: &str) -> Result<(), String> {
        Ok(())
    }

    fn undeclared_holds(&self) -> Result<std::collections::BTreeMap<String, String>, String> {
        Ok(std::collections::BTreeMap::new())
    }
}

/// A rotation authority whose durable record cannot be written.
struct RefusingRotationControl;

impl ListenerRotationControl for RefusingRotationControl {
    fn pending(&self) -> Result<Option<ListenerRotationStatus>, String> {
        Ok(None)
    }

    fn regenerate(&self, _started_at_unix_ms: u64) -> Result<ListenerRotationStatus, String> {
        Err("the rotation record could not be written".into())
    }

    fn force_cutover(&self) -> Result<Vec<String>, String> {
        Err("the rotation record could not be written".into())
    }

    fn update_endpoints(&self, _endpoints: Vec<String>) -> Result<(), String> {
        Err("the rotation record could not be written".into())
    }

    fn hold_for_connection(&self, _executor_id: &str, _display_name: &str) -> Result<(), String> {
        Err("the rotation record could not be written".into())
    }

    fn undeclared_holds(&self) -> Result<std::collections::BTreeMap<String, String>, String> {
        Ok(std::collections::BTreeMap::new())
    }
}

/// The hold is the safety boundary, so a hold that cannot be persisted has
/// to refuse the connection rather than be logged past. A machine admitted
/// without one is handed the staged advertisement anyway and can then be
/// cut over behind — exactly what the record exists to prevent — and no
/// amount of logging keeps it from being stranded.
#[test]
fn a_connection_is_refused_when_its_cutover_hold_cannot_be_recorded() {
    let fleet = Fleet::default();
    fleet.register_listener_rotation_control(Arc::new(RefusingRotationControl));
    let (_, entry) = fleet_entry("dialed-in", "linux", 0, &[]);

    assert!(fleet
        .attach_advertised_executor(
            Arc::unwrap_or_clone(entry.advertisement),
            Default::default(),
            entry.sender,
            false,
            None,
            None
        )
        .is_err());

    // Refused outright rather than half-attached: nothing may be sent a
    // staged pin it is not on the hook for, and a caller that sees an error
    // must not find the machine attached anyway.
    assert_eq!(fleet.live_generation("dialed-in"), None);
    assert!(fleet.listener_rotation_gate_executor_ids().is_empty());
    assert!(fleet.undeclared_connected_executors().is_empty());
}

/// The colocated machine pins no listener certificate and places no hold,
/// so a rotation record that cannot be written has no bearing on it.
#[test]
fn a_colocated_attach_survives_a_rotation_record_that_cannot_be_written() {
    let fleet = Fleet::default();
    fleet.register_listener_rotation_control(Arc::new(RefusingRotationControl));
    let (sender, _receiver) = mpsc::unbounded_channel();

    assert!(fleet.attach_executor(sender) > 0);
    assert_eq!(
        fleet.live_generation(COLOCATED_EXECUTOR_ID),
        Some(1),
        "the colocated link attaches regardless of the rotation record"
    );
}

#[test]
fn listener_rotation_operator_calls_delegate_to_the_registered_authority() {
    let fleet = Fleet::default();
    assert_eq!(fleet.listener_rotation_status().unwrap(), None);
    assert!(fleet.regenerate_listener_certificate().is_err());
    assert!(fleet.force_listener_rotation_cutover().is_err());

    let status = ListenerRotationStatus {
        staged_fingerprint: "new-fingerprint".into(),
        started_at_unix_ms: 42,
        pending_executor_ids: vec!["bglab-ub".into(), "bglab-win".into()],
    };
    let control = Arc::new(TestListenerRotationControl {
        regenerate_at: AtomicU64::new(0),
        status: status.clone(),
        casualties: status.pending_executor_ids.clone(),
        endpoints: Mutex::new(Vec::new()),
    });
    fleet.register_listener_rotation_control(control.clone());

    assert_eq!(
        fleet.listener_rotation_status().unwrap(),
        Some(status.clone())
    );
    assert_eq!(fleet.regenerate_listener_certificate().unwrap(), status);
    assert!(control.regenerate_at.load(Ordering::SeqCst) > 0);
    assert_eq!(
        fleet.force_listener_rotation_cutover().unwrap(),
        vec!["bglab-ub", "bglab-win"]
    );
    fleet
        .update_listener_endpoints(vec!["wss://runner.example/api/executor".into()])
        .unwrap();
    assert_eq!(
        *control.endpoints.lock().unwrap(),
        vec!["wss://runner.example/api/executor"]
    );
}

#[test]
fn pending_listener_rotation_names_each_machine_and_its_presence_cause() {
    let fleet = Fleet::default();
    fleet.declare_enrolled_remote("executor-a", "bglab-ub", "linux", "x86_64");
    fleet.declare_enrolled_remote("executor-b", "bglab-win", "windows", "x86_64");
    fleet.record_remote_attach_attempt(
        "executor-b",
        RemoteLinkState::NeverConnected {
            since_bootstrap_unix_ms: 42,
        },
        RemoteAttachAttempt::bootstrap_failure(42, "installed"),
    );

    let (executor_id, entry) = fleet_entry("executor-c", "linux", 0, &[]);
    fleet.lock_connections().insert(executor_id, entry);

    let pending = fleet.listener_rotation_pending_machines(&[
        "executor-a".into(),
        "executor-b".into(),
        "executor-c".into(),
        "executor-d".into(),
    ]);
    assert_eq!(pending[0].name, "bglab-ub");
    assert!(pending[0].presence_cause.contains("not seen"));
    assert_eq!(pending[1].name, "bglab-win");
    assert_eq!(
        pending[1].presence_cause,
        "installed; waiting for first connection"
    );
    // Connected but never declared: described by its own advertisement,
    // because the live socket is the whole account of it.
    assert_eq!(pending[2].name, "executor-c");
    assert_eq!(
        pending[2].presence_cause,
        "connected; acknowledgement has not arrived"
    );
    // Neither enrolled nor connected: the gate was read against a fleet
    // that has since changed.
    assert_eq!(pending[3].name, "executor-d");
    assert_eq!(
        pending[3].presence_cause,
        "enrollment changed while status was read"
    );
}

/// The claim the whole design rests on, exercised as composition rather
/// than as bookkeeping: a successor that has won a question does not get to
/// run until the answer it displaced is confirmed dead.
///
/// Asserting only that the registry shows one holder would pass against the
/// exact defect this guards. The registry showing one item is compatible
/// with two items running — that is precisely what a fire-and-forget cancel
/// produces — so what is asserted here is that the gate is still closed
/// while the death is unconfirmed, and opens on the terminal outcome and
/// nothing else.
#[tokio::test]
async fn a_successor_does_not_run_until_what_it_displaced_is_confirmed_dead() {
    let fleet = Fleet::default();
    let key = purpose::PurposeKey::main_cadence("project", "rust-tests", "linux");
    let older: RequestIdentity = ("tip-x".into(), "tip-x-attempt".into());
    let newer: RequestIdentity = ("tip-x1".into(), "tip-x1-attempt".into());

    fleet
        .purposes
        .lock()
        .unwrap()
        .admit(key.clone(), older.clone(), 10);
    fleet
        .purposes
        .lock()
        .unwrap()
        .admit(key.clone(), newer.clone(), 20);

    let mut waiting = {
        let fleet = fleet.clone();
        let (key, newer) = (key.clone(), newer.clone());
        tokio::spawn(async move { fleet.await_purpose_quiescent(&key, &newer).await })
    };

    // Winning the key is not permission to dispatch. This also parks the
    // waiter on its wakeup, so the notification below cannot be delivered
    // before anyone is listening for it.
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut waiting)
            .await
            .is_err(),
        "the successor must not proceed while the item it displaced is still alive"
    );

    // The predecessor's terminal outcome arrives, exactly as the leader
    // task delivers it.
    fleet.purposes.lock().unwrap().finish(&older);
    fleet.purpose_quiesced.notify_waiters();

    let gate = tokio::time::timeout(std::time::Duration::from_secs(5), waiting)
        .await
        .expect("a confirmed death must release the successor")
        .expect("the waiting task must not panic");
    assert!(matches!(gate, PurposeGate::Clear));
}

/// The burst this issue exists for: three tips in quick succession, where
/// the middle one is displaced while it is still waiting to dispatch.
///
/// Being displaced is what puts an item into the debt set, so a gate that
/// only asked "does this question owe anything" would have the middle item
/// waiting on itself — and it could only leave that set by completing work
/// the gate was holding it back from starting. The deadlock was real,
/// reachable from an ordinary merge stream, and cost the newest tip the full
/// budget on a request that had provably never run.
#[tokio::test]
async fn an_item_displaced_while_waiting_to_dispatch_settles_instead_of_deadlocking() {
    let fleet = Fleet::default();
    let key = purpose::PurposeKey::main_cadence("project", "rust-tests", "linux");
    let a: RequestIdentity = ("tip-a".into(), "a".into());
    let b: RequestIdentity = ("tip-b".into(), "b".into());
    let c: RequestIdentity = ("tip-c".into(), "c".into());

    fleet
        .purposes
        .lock()
        .unwrap()
        .admit(key.clone(), a.clone(), 10);
    fleet
        .purposes
        .lock()
        .unwrap()
        .admit(key.clone(), b.clone(), 20);

    // B holds the question and waits for A's death.
    let mut b_gate = {
        let fleet = fleet.clone();
        let (key, b) = (key.clone(), b.clone());
        tokio::spawn(async move { fleet.await_purpose_quiescent(&key, &b).await })
    };
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut b_gate)
            .await
            .is_err(),
        "B must wait for A"
    );

    // C arrives and takes the question from B, which is still at the gate.
    fleet
        .purposes
        .lock()
        .unwrap()
        .admit(key.clone(), c.clone(), 30);
    fleet.purpose_quiesced.notify_waiters();

    let gate = tokio::time::timeout(std::time::Duration::from_secs(5), b_gate)
        .await
        .expect("a displaced waiter must not wait out the budget on itself")
        .expect("the waiting task must not panic");
    assert!(
        matches!(gate, PurposeGate::Displaced),
        "B never dispatched, so it is settled rather than owed a death"
    );

    // B settled its own debt on the way out, so the question owes only A.
    let registry = fleet.purposes.lock().unwrap();
    assert_eq!(registry.pending_retirements(&key), 1);
    assert!(registry.is_live_holder(&key, &c));
}

/// A debt that never clears refuses the successor rather than dispatching
/// beside it.
///
/// Proceeding would knowingly create the overlap this whole design exists to
/// prevent, on the reasoning that a logged overlap is better than a stall.
/// It is not: a warning is a record, not a decision anyone made, and the
/// operator symptom of that overlap is exactly what opened this issue. A
/// refusal is visible, is re-armed by the next cadence, and never puts two
/// answers to one question on the fleet.
#[tokio::test(start_paused = true)]
async fn a_debt_that_never_clears_refuses_rather_than_dispatching_beside_it() {
    let fleet = Fleet::default();
    let key = purpose::PurposeKey::main_cadence("project", "rust-tests", "linux");
    let newer: RequestIdentity = ("tip-x1".into(), "attempt".into());
    fleet
        .purposes
        .lock()
        .unwrap()
        .admit(key.clone(), ("tip-x".into(), "attempt".into()), 10);
    fleet
        .purposes
        .lock()
        .unwrap()
        .admit(key.clone(), newer.clone(), 20);

    // Nothing ever clears the debt; the budget is what returns.
    let gate = fleet.await_purpose_quiescent(&key, &newer).await;
    assert!(
        matches!(gate, PurposeGate::Owed { .. }),
        "an unpaid debt must refuse the successor, not wave it through"
    );
    assert_eq!(
        fleet.purposes.lock().unwrap().pending_retirements(&key),
        1,
        "the debt is still owed; the gate never pretended it was paid"
    );
}

/// A question with nothing outstanding does not make its next answer wait.
#[tokio::test]
async fn an_uncontested_question_dispatches_without_waiting() {
    let fleet = Fleet::default();
    let key = purpose::PurposeKey::main_cadence("project", "rust-tests", "linux");
    fleet
        .purposes
        .lock()
        .unwrap()
        .admit(key.clone(), ("tip-x".into(), "attempt".into()), 10);

    let gate = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        fleet.await_purpose_quiescent(&key, &("tip-x".into(), "attempt".into())),
    )
    .await
    .expect("a question owing no deaths must not delay its own answer");
    assert!(matches!(gate, PurposeGate::Clear));
}

/// The chokepoint's whole promise, composed rather than in pieces: arming a
/// newer answer to a question retires the older one, and the retirement is
/// recorded where the pre-submit gate will find it.
///
/// The phase the displaced item had reached is deliberately not part of the
/// condition. An item still being placed has no executor to tell, so the
/// record alone must stop it — that is what closes the window in which a
/// superseded wave used to dispatch anyway and then chase itself with a
/// cancel it could lose.
#[test]
fn arming_a_newer_answer_retires_the_older_before_it_can_dispatch() {
    let fleet = Fleet::default();
    let key = purpose::PurposeKey::main_cadence("project", "rust-tests", "linux");
    let older: RequestIdentity = ("tip-x".into(), "tip-x-attempt".into());
    let newer: RequestIdentity = ("tip-x1".into(), "tip-x1-attempt".into());

    assert_eq!(
        fleet
            .purposes
            .lock()
            .unwrap()
            .admit(key.clone(), older.clone(), 10),
        purpose::Admission::Admitted
    );
    let admission = fleet
        .purposes
        .lock()
        .unwrap()
        .admit(key.clone(), newer.clone(), 20);
    let purpose::Admission::Replaced { retire, .. } = admission else {
        panic!("a newer answer to one question must displace the older, got {admission:?}");
    };
    assert_eq!(retire, older);

    fleet.retire_item(&retire, CancelCause::Superseded);
    assert!(
        fleet.cancelled_leaders.lock().unwrap().contains(&older),
        "a retirement must be recorded even with no executor to deliver it to"
    );

    // The gate consumes the record, and consumes it once: a later item that
    // was never retired must not inherit this one's cancellation.
    assert!(fleet.cancelled_leaders.lock().unwrap().remove(&older));
    assert!(!fleet.cancelled_leaders.lock().unwrap().contains(&newer));
}

/// A stale item is refused by the registry rather than left to race, so the
/// key still names the newest answer afterwards.
#[test]
fn an_answer_older_than_the_live_one_never_becomes_the_live_one() {
    let fleet = Fleet::default();
    let key = purpose::PurposeKey::main_cadence("project", "rust-tests", "linux");
    let newer: RequestIdentity = ("newer".into(), "newer-attempt".into());
    let older: RequestIdentity = ("older".into(), "older-attempt".into());

    fleet
        .purposes
        .lock()
        .unwrap()
        .admit(key.clone(), newer.clone(), 50);
    assert_eq!(
        fleet
            .purposes
            .lock()
            .unwrap()
            .admit(key.clone(), older.clone(), 20),
        purpose::Admission::Superseded {
            holder: newer.clone()
        }
    );
    assert!(
        !fleet.cancelled_leaders.lock().unwrap().contains(&newer),
        "refusing a stale arrival must not retire the live item it lost to"
    );
}

fn debt_key() -> purpose::PurposeKey {
    purpose::PurposeKey::main_cadence("project", "rust-tests", "linux")
}

/// An unconfirmed debt on `executor_id`: granted there, then abandoned by
/// the runner without the machine answering.
fn unconfirmed_debt_on(
    fleet: &Fleet,
    id: &str,
    executor_id: &str,
    generation: u64,
    at: u64,
) -> RequestIdentity {
    let item: RequestIdentity = (id.into(), format!("{id}-attempt"));
    let mut registry = fleet.purposes.lock().unwrap();
    registry.admit(debt_key(), item.clone(), at);
    registry.dispatched(&item, executor_id, generation, at);
    registry.end_unconfirmed(&item, purpose::UnconfirmedReason::LeaderVanished, at);
    item
}

pub(super) fn completion_of(
    item: &RequestIdentity,
) -> cairn_common::executor_protocol::CellCompletion {
    cairn_common::executor_protocol::CellCompletion {
        executor_id: String::new(),
        request_id: item.0.clone(),
        attempt_id: item.1.clone(),
        owner: None,
        command_class: cairn_common::executor_protocol::CellCommandClass::classify("true"),
        command: "true".into(),
        priority: CellPriority::ReviewCheck,
        queued_at_unix_ms: 0,
        started_at_unix_ms: None,
        finished_at_unix_ms: 0,
        duration_ms: 0,
        verdict: cairn_common::executor_protocol::CellCompletionVerdict::Cancelled,
        failure_output_tail: None,
        transcript_event_uri: None,
        resource_reservation: None,
        learned_estimate: None,
        actuals: None,
        cached: false,
        subscriber_count: 1,
        served_at_unix_ms: 0,
        result_evicted: None,
    }
}

/// The cairn/4482 specimen. Work retired before any machine claimed it is
/// the runner's to end, and it ends now: waiting for a claim that may
/// never come is what left a question owing a death for hours. The
/// `Cancelled` it ends with is its own terminal outcome, so its successor
/// is free.
#[tokio::test]
async fn retiring_offered_work_before_its_grant_ends_it_and_tells_the_machine() {
    let temp = tempfile::tempdir().unwrap();
    let orch = Arc::new(test_orchestrator(temp.path()).await);
    let request = provenance_request();
    let old: RequestIdentity = (request.request_id.clone(), request.attempt_id.clone());
    let newer: RequestIdentity = ("successor".into(), "successor-attempt".into());
    orch.fleet
        .purposes
        .lock()
        .unwrap()
        .admit(debt_key(), old.clone(), 10);
    let (_generation, submitting, mut machine, offered) = offered_and_held(&orch).await;

    let admission = orch
        .fleet
        .purposes
        .lock()
        .unwrap()
        .admit(debt_key(), newer.clone(), 20);
    assert!(matches!(admission, purpose::Admission::Replaced { .. }));
    orch.fleet.retire_item(&old, CancelCause::Superseded);

    let executed = tokio::time::timeout(Duration::from_secs(5), submitting)
        .await
        .expect("retired work the runner holds ends without any machine's help")
        .unwrap();
    assert!(
        matches!(executed.outcome, CellOutcome::Cancelled { .. }),
        "{:?}",
        executed.outcome
    );
    assert!(executed.abandoned_grant.is_none(), "nothing was granted");
    loop {
        match tokio::time::timeout(Duration::from_secs(5), machine.recv()).await {
            Ok(Some(ExecutorMessage::Lost { entry_id })) => {
                assert_eq!(entry_id, offered);
                break;
            }
            Ok(Some(_)) => continue,
            other => panic!("the machine holding the offer was never told: {other:?}"),
        }
    }
    assert_eq!(orch.fleet.queue.len(), 0);

    orch.fleet
        .settle_leader_purpose(&old, executed.abandoned_grant);
    let gate = tokio::time::timeout(
        Duration::from_secs(5),
        orch.fleet.await_purpose_quiescent(&debt_key(), &newer),
    )
    .await
    .expect("the successor is not held for a death that already happened");
    assert!(matches!(gate, PurposeGate::Clear));
}

#[tokio::test]
async fn cancelling_unclaimed_work_parked_on_an_update_prevents_later_placement() {
    let temp = tempfile::tempdir().unwrap();
    let orch = Arc::new(test_orchestrator(temp.path()).await);
    let (sender, mut machine) = mpsc::unbounded_channel();
    let generation = orch.fleet.attach_announced_executor(sender);
    orch.fleet
        .set_update_hold_for_test(COLOCATED_EXECUTOR_ID, true);
    let request = provenance_request();
    let identity = (request.request_id.clone(), request.attempt_id.clone());
    let submitting = scratch_submission_with_stop(&orch, None, false).await;
    let entry_id = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(entry) = orch
                .fleet
                .queue
                .with_entry_by_identity(&identity, |entry| entry.id.clone())
            {
                break entry;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the pinned batch is queued");
    // Long enough to have joined the dispatcher and been matched against the
    // room the machine announced, were the update hold not keeping it off.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !std::iter::from_fn(|| machine.try_recv().ok())
            .any(|frame| matches!(frame, ExecutorMessage::Offer { .. })),
        "the pinned batch parks on the update without an offer"
    );
    assert!(orch
        .fleet
        .claimed_executor_of(&request.request_id)
        .is_none());
    assert_eq!(
        orch.fleet.cancel_request(&request.request_id),
        CancelOutcome::Stopping
    );
    let executed = tokio::time::timeout(Duration::from_secs(5), submitting)
        .await
        .expect("an unclaimed cancellation needs no machine confirmation")
        .unwrap();
    assert!(matches!(executed.outcome, CellOutcome::Cancelled { .. }));
    assert!(executed.account.placement.is_none());
    assert!(executed.grant_without_result.is_none());
    assert_eq!(orch.fleet.queue.len(), 0);

    orch.fleet
        .set_update_hold_for_test(COLOCATED_EXECUTOR_ID, false);
    // Even an already-sent, late Claim cannot revive a withdrawn entry.
    orch.fleet.handle_executor_message(
        COLOCATED_EXECUTOR_ID,
        generation,
        ExecutorMessage::Claim { entry_id },
    );
    while let Ok(frame) = machine.try_recv() {
        assert!(
            !matches!(
                frame,
                ExecutorMessage::Offer { .. } | ExecutorMessage::Granted { .. }
            ),
            "cancelled work was dispatched after capacity returned: {frame:?}"
        );
    }
    assert_eq!(orch.fleet.queue.len(), 0);
}

#[tokio::test]
async fn a_batch_stop_before_grant_withdraws_but_after_grant_waits_for_the_machine() {
    for grant_first in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let orch = Arc::new(test_orchestrator(temp.path()).await);
        let (sender, mut machine) = mpsc::unbounded_channel();
        let generation = orch.fleet.attach_announced_executor(sender);
        let stop = Arc::new(PlacementStop::default());
        let submitting = scratch_submission_with_stop(&orch, Some(stop.clone()), true).await;
        let offered = loop {
            match tokio::time::timeout(Duration::from_secs(5), machine.recv()).await {
                Ok(Some(ExecutorMessage::Offer { entry_id, .. })) => break entry_id,
                Ok(Some(_)) => {}
                other => panic!("expected offer, got {other:?}"),
            }
        };
        orch.fleet.handle_executor_message(
            COLOCATED_EXECUTOR_ID,
            generation,
            ExecutorMessage::Claim { entry_id: offered },
        );
        if grant_first {
            loop {
                match tokio::time::timeout(Duration::from_secs(5), machine.recv()).await {
                    Ok(Some(ExecutorMessage::Granted { .. })) => break,
                    Ok(Some(_)) => {}
                    other => panic!("expected grant, got {other:?}"),
                }
            }
        }
        // No yield between Claim and stop in the pre-grant case: this
        // exercises the grant gate even when a claim is already ready.
        stop.stop();
        let request = provenance_request();
        assert_eq!(
            orch.fleet.cancel_request(&request.request_id),
            CancelOutcome::Stopping
        );
        if grant_first {
            loop {
                match tokio::time::timeout(Duration::from_secs(5), machine.recv()).await {
                    Ok(Some(ExecutorMessage::Cancel { .. })) => break,
                    Ok(Some(_)) => {}
                    other => panic!("expected machine cancel, got {other:?}"),
                }
            }
            assert!(
                !submitting.is_finished(),
                "sending Cancel is not a terminal report"
            );
            assert_eq!(orch.fleet.queue.len(), 1);
            orch.fleet.handle_executor_message(
                COLOCATED_EXECUTOR_ID,
                generation,
                ExecutorMessage::Result {
                    request_id: request.request_id.clone(),
                    attempt_id: request.attempt_id.clone(),
                    outcome: CellOutcome::Cancelled {
                        request_id: request.request_id.clone(),
                        attempt_id: request.attempt_id.clone(),
                    },
                },
            );
        }
        let executed = tokio::time::timeout(Duration::from_secs(5), submitting)
            .await
            .expect("the cancellation settles")
            .unwrap();
        assert!(matches!(executed.outcome, CellOutcome::Cancelled { .. }));
        assert_eq!(executed.account.placement.is_some(), grant_first);
        assert!(executed.grant_without_result.is_none());
        assert_eq!(orch.fleet.queue.len(), 0);
        if !grant_first {
            while let Ok(frame) = machine.try_recv() {
                assert!(
                    !matches!(frame, ExecutorMessage::Granted { .. }),
                    "{frame:?}"
                );
            }
        }
    }
}

/// Work retired while it is still being prepared is never queued at all.
#[tokio::test]
async fn work_retired_before_it_is_queued_never_enters_the_queue() {
    let temp = tempfile::tempdir().unwrap();
    let orch = Arc::new(test_orchestrator(temp.path()).await);
    let request = provenance_request();
    let old: RequestIdentity = (request.request_id.clone(), request.attempt_id.clone());
    orch.fleet
        .purposes
        .lock()
        .unwrap()
        .admit(debt_key(), old.clone(), 10);
    orch.fleet.retire_item(&old, CancelCause::Superseded);

    let executed = tokio::time::timeout(Duration::from_secs(5), scratch_submission(&orch).await)
        .await
        .expect("work retired before queueing ends at once")
        .unwrap();
    assert!(
        matches!(executed.outcome, CellOutcome::Cancelled { .. }),
        "{:?}",
        executed.outcome
    );
    assert_eq!(orch.fleet.queue.len(), 0);
    assert!(!orch.fleet.cancelled_leaders.lock().unwrap().contains(&old));
}

/// Revoking a machine's enrollment is the same evidence as removing it,
/// attached or not. A detached machine has no link to end, and its debts
/// would otherwise wait for a snapshot it can never send.
#[tokio::test]
async fn revoking_a_detached_machines_enrollment_settles_its_debts() {
    let temp = tempfile::tempdir().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    unconfirmed_debt_on(&orch.fleet, "held", "detached-remote", 2, 10);
    unconfirmed_debt_on(&orch.fleet, "elsewhere", "other-remote", 1, 20);

    management::enrollment_revoked(&orch, "detached-remote", "device-1");

    let left = orch.fleet.purpose_debts();
    assert_eq!(left.len(), 1, "{left:?}");
    assert_eq!(
        left[0].placement.as_ref().unwrap().executor_id,
        "other-remote",
        "revoking one machine settles only its debts"
    );
}

/// A runner that stops waiting on a grant has learned nothing about the
/// machine. The question keeps owing the death, named against that
/// machine, until the machine itself says the attempt ended. A later
/// report from the same connection that merely omits the attempt is not
/// that statement.
#[tokio::test]
async fn a_grant_the_runner_gave_up_on_leaves_its_question_owed() {
    let temp = tempfile::tempdir().unwrap();
    let orch = Arc::new(test_orchestrator(temp.path()).await);
    let request = provenance_request();
    let item: RequestIdentity = (request.request_id.clone(), request.attempt_id.clone());
    orch.fleet
        .purposes
        .lock()
        .unwrap()
        .admit(debt_key(), item.clone(), 10);
    let (generation, submitting, _machine) = granted_and_held(&orch).await;

    let executed = settled(submitting).await;
    assert!(
        executed.abandoned_grant.is_some(),
        "the silence watchdog ended the wait, not the machine"
    );
    orch.fleet
        .settle_leader_purpose(&item, executed.abandoned_grant);
    let debts = orch.fleet.purpose_debts();
    assert_eq!(debts.len(), 1);
    assert_eq!(
        debts[0].placement.as_ref().map(|p| p.executor_id.as_str()),
        Some(COLOCATED_EXECUTOR_ID)
    );
    assert!(
        matches!(
            &debts[0].confirmation,
            purpose::Confirmation::Unconfirmed {
                reason: purpose::UnconfirmedReason::GrantAbandoned { .. },
                ..
            }
        ),
        "{:?}",
        debts[0].confirmation
    );

    for _ in 0..2 {
        orch.fleet.apply_executor_snapshot(
            COLOCATED_EXECUTOR_ID,
            generation,
            FleetSnapshot::default(),
            ExecutorSubstrateReport::default(),
        );
    }
    assert!(
        orch.fleet.purpose_owes(&debt_key()),
        "a report from the grant's own connection can cross the grant in flight"
    );

    let mut ended = FleetSnapshot::default();
    ended.recent_completions.push(completion_of(&item));
    orch.fleet.apply_executor_snapshot(
        COLOCATED_EXECUTOR_ID,
        generation,
        ended,
        ExecutorSubstrateReport::default(),
    );
    assert!(!orch.fleet.purpose_owes(&debt_key()));
}

/// Each kind of machine evidence settles an unconfirmed debt, and the two
/// things that look like evidence and are not leave it standing.
#[test]
fn machine_evidence_settles_an_unconfirmed_debt_and_nothing_else_does() {
    let fleet = Fleet::default();
    let owes = |fleet: &Fleet| {
        fleet
            .purposes
            .lock()
            .unwrap()
            .pending_retirements(&debt_key())
    };

    unconfirmed_debt_on(&fleet, "a", "remote-x", 3, 10);
    fleet.settle_purpose_debts_from_account("remote-x", 3, false, &FleetSnapshot::default());
    fleet.settle_purpose_debts_from_account("remote-x", 3, true, &FleetSnapshot::default());
    assert_eq!(
        owes(&fleet),
        1,
        "the grant's own connection proves nothing by omission"
    );
    fleet.settle_purpose_debts_from_account("remote-x", 4, false, &FleetSnapshot::default());
    assert_eq!(
        owes(&fleet),
        1,
        "only a newer connection's first report is an account"
    );
    fleet.settle_purpose_debts_from_account("remote-x", 4, true, &FleetSnapshot::default());
    assert_eq!(
        owes(&fleet),
        0,
        "a newer connection that does not list it settles it"
    );

    let b = unconfirmed_debt_on(&fleet, "b", "remote-x", 4, 20);
    let mut listing = FleetSnapshot::default();
    listing.recent_completions.push(completion_of(&b));
    fleet.settle_purpose_debts_from_account("remote-x", 4, false, &listing);
    assert_eq!(owes(&fleet), 0, "the machine said it ended");

    unconfirmed_debt_on(&fleet, "c", "remote-x", 5, 30);
    fleet.end_link_generation("remote-x", 5, &ExecutorDisconnectOrigin::PeerOrIo);
    assert_eq!(
        owes(&fleet),
        1,
        "a link that went away is not a machine that did"
    );
    fleet.end_link_generation("remote-x", 5, &ExecutorDisconnectOrigin::RunnerInitiated);
    assert_eq!(owes(&fleet), 0, "the runner recycled the machine's process");

    unconfirmed_debt_on(&fleet, "d", "remote-x", 6, 40);
    unconfirmed_debt_on(&fleet, "e", "remote-y", 1, 50);
    fleet.abandon_held_executions("remote-x", "remote-x was removed");
    assert_eq!(
        owes(&fleet),
        1,
        "removing one machine settles only its debts"
    );
}

/// A leader that vanishes before its grant owed nothing; one that vanishes
/// after leaves its question owing, named against the machine.
#[test]
fn a_vanished_leader_settles_before_a_grant_and_leaves_its_debt_after() {
    let fleet = Fleet::default();
    let guard = |leader: &RequestIdentity| CoalescedLeaderCompletionGuard {
        pool: fleet.clone(),
        leader: leader.clone(),
        work_class: provenance_request().placement_work_class,
        keys: Vec::new(),
        runner_context_id: None,
        armed: true,
    };
    let before: RequestIdentity = ("before".into(), "a".into());
    fleet
        .purposes
        .lock()
        .unwrap()
        .admit(debt_key(), before.clone(), 10);
    drop(guard(&before));
    assert!(fleet.purpose_debts().is_empty());
    assert!(!fleet
        .purposes
        .lock()
        .unwrap()
        .is_live_holder(&debt_key(), &before));

    let after: RequestIdentity = ("after".into(), "a".into());
    {
        let mut registry = fleet.purposes.lock().unwrap();
        registry.admit(debt_key(), after.clone(), 20);
        registry.dispatched(&after, "remote-x", 2, 21);
    }
    drop(guard(&after));
    let debts = fleet.purpose_debts();
    assert_eq!(debts.len(), 1);
    assert_eq!(
        debts[0].confirmation,
        purpose::Confirmation::Unconfirmed {
            since_unix_ms: debts[0].retired_at_unix_ms,
            reason: purpose::UnconfirmedReason::LeaderVanished,
        }
    );
}

/// A purpose item that only joined another leader's execution has no
/// leader of its own to settle it. Retiring it ends its subscription, and
/// with it the item; the execution it was the last to want then becomes
/// the question's debt, cleared by that execution's own leader.
#[tokio::test]
async fn a_retired_joiner_ends_its_subscription_and_owes_the_execution_it_retired() {
    let fleet = Fleet::default();
    let joiner: RequestIdentity = ("joiner".into(), "joiner-attempt".into());
    let joined_leader: RequestIdentity = ("other".into(), "other-attempt".into());
    let subscription: RequestIdentity = ("joiner:check-0".into(), "joiner-attempt:check-0".into());
    let (waiter, delivered) = oneshot::channel();
    fleet.in_flight.lock().unwrap().open(
        CoalescingKey::new(result_identity(), joined_leader.clone()),
        PlacementDemand::default(),
        HashMap::from([(
            subscription.clone(),
            CoalescedSubscriber {
                waiter,
                priority: CellPriority::ReviewCheck,
                requesting_job_id: None,
            },
        )]),
    );
    fleet
        .purposes
        .lock()
        .unwrap()
        .admit(debt_key(), joiner.clone(), 10);
    let admission_guard = PurposeAdmissionGuard {
        pool: fleet.clone(),
        item: joiner.clone(),
        armed: true,
    };
    fleet
        .purpose_joiners
        .lock()
        .unwrap()
        .insert(joiner.clone(), (debt_key(), vec![subscription]));

    let successor: RequestIdentity = ("successor".into(), "successor-attempt".into());
    fleet
        .purposes
        .lock()
        .unwrap()
        .admit(debt_key(), successor, 20);
    fleet.retire_item(&joiner, CancelCause::Superseded);
    assert!(
        matches!(
            delivered.await,
            Ok(Err(CoalescedWaitError::Cancelled(CancelCause::Superseded)))
        ),
        "the joiner's own wait ends at once"
    );
    drop(admission_guard);

    let debts = fleet.purpose_debts();
    assert_eq!(
        debts.iter().map(|debt| &debt.request).collect::<Vec<_>>(),
        vec![&joined_leader],
        "the joiner settled; the execution it retired is owed in its place"
    );
    fleet.settle_leader_purpose(&joined_leader, None);
    assert!(fleet.purpose_debts().is_empty());
}

/// The refusal is the operator's only view of a blocked question, so it
/// names the machine, where the item had got to, and a restart.
#[test]
fn the_refusal_names_the_machine_the_phase_and_a_restart() {
    let fleet = Fleet::default();
    fleet.purposes.lock().unwrap().reload(
        vec![purpose::PurposeDebtRow {
            key: debt_key(),
            request: ("stranded".into(), "stranded-attempt".into()),
            state: purpose::DebtRowState::Retiring,
            phase: purpose::ItemPhase::Dispatched,
            placement: purpose::DebtPlacement {
                executor_id: "remote-x".into(),
                generation: 7,
                dispatched_at_unix_ms: 1_000,
                awaiting_post_boot_account: false,
            },
            cause: Some(purpose::RetirementCause::Displaced {
                by: ("newer".into(), "newer-attempt".into()),
            }),
            confirmation: Some(purpose::Confirmation::Awaiting),
            retired_at_unix_ms: Some(2_000),
        }],
        3_000,
    );
    let diagnostic = fleet.purpose_refusal_diagnostic(&debt_key(), &fleet.purpose_debts());
    for expected in [
        "request stranded attempt stranded-attempt",
        "on remote-x",
        "generation 7",
        "after it was granted to a machine",
        "displaced by request newer",
        "runner restarted at",
        "authorizes the overlap",
    ] {
        assert!(
            diagnostic.contains(expected),
            "missing {expected:?}: {diagnostic}"
        );
    }
}

use crate::db::DbState;
use crate::services::testing::TestServicesBuilder;
use crate::storage::{LocalDb, SearchIndex};
use cairn_common::executor_protocol::{
    CellOccupancy, GitObjectFormat, MachineMemory, ResidencyFence, ResidentOccupancyEvidence,
    WarmRepositoryRoots, WarmRootChange, WarmRootInventory, EXECUTOR_LINK_STALL_REMEDIATION_MS,
};
use cairn_common::wire_vocabulary::{CellAdmissionKind, Measurement};

fn relay_request(run_id: Option<&str>) -> cairn_common::protocol::CallbackRequest {
    cairn_common::protocol::CallbackRequest {
        cwd: "/tmp/worktree".into(),
        run_id: run_id.map(str::to_string),
        tool: "read".into(),
        payload: serde_json::json!({"paths": ["cairn:~/todos"]}),
        ..Default::default()
    }
}

fn insert_bound_relay_context(pool: &Fleet) {
    pool.runner_contexts.lock().unwrap().insert(
        "context".into(),
        RunnerContextEntry::new(
            "context",
            "request-1:attempt-1",
            RunnerCallbackContext {
                request: Some(relay_request(Some("run-1"))),
                run_context: None,
                check_status_board: None,
                run_batch_request_id: None,
                live_checkout: false,
                item_settlements: None,
                executor_binding: Some(RunnerContextExecutorBinding {
                    executor_id: "executor-1".into(),
                    link: BatchLink::Bound(7),
                    request_id: "request-1".into(),
                    attempt_id: "attempt-1".into(),
                }),
                output: output_guard::RelayedOutput::shared(),
            },
        ),
    );
}

/// Overlapping remote item reports update the live batch from placement to
/// parallel execution, then the remaining item, and finally settlement.
#[tokio::test]
async fn a_run_batch_item_report_marks_the_item_running_on_its_live_control() {
    use crate::mcp::handlers::run::detached::{self, BatchProgress};
    let dir = tempfile::tempdir().unwrap();
    let orch = test_orchestrator(dir.path()).await;
    let request_id = uuid::Uuid::new_v4().to_string();
    let live = detached::LiveBatch::claim_for_test(&request_id);
    let _claimed = orch.fleet.claim_job_command_for_test(&request_id, "job", 1);
    assert_eq!(
        detached::live_progress(&orch, &request_id)
            .unwrap()
            .describe(0),
        "placed on `local`; it has not reported per-item progress"
    );
    orch.fleet.runner_contexts.lock().unwrap().insert(
        "run-batch-context".into(),
        RunnerContextEntry::new(
            "run-batch-context",
            &format!("{request_id}:attempt-1"),
            RunnerCallbackContext {
                request: None,
                run_context: None,
                check_status_board: None,
                run_batch_request_id: Some(request_id.clone()),
                live_checkout: false,
                executor_binding: None,
                item_settlements: None,
                output: output_guard::RelayedOutput::shared(),
            },
        ),
    );
    orch.fleet
        .handle_runner_callback(
            &orch,
            RunnerCallback::ProcessItemStarted {
                runner_context_id: "run-batch-context".into(),
                stream_id: "tool-1:0".into(),
            },
        )
        .await;
    assert!(
        matches!(
            detached::live_progress(&orch, &request_id),
            Some(BatchProgress::Running {
                index: 0,
                total: 1,
                ..
            })
        ),
        "{:?}",
        detached::live_progress(&orch, &request_id)
    );
    orch.fleet
        .handle_runner_callback(
            &orch,
            RunnerCallback::ProcessItemStarted {
                runner_context_id: "run-batch-context".into(),
                stream_id: "tool-1:2".into(),
            },
        )
        .await;
    assert!(matches!(
        detached::live_progress(&orch, &request_id),
        Some(BatchProgress::Parallel { running: 2, .. })
    ));
    orch.fleet
        .handle_runner_callback(
            &orch,
            RunnerCallback::ProcessItemCompleted {
                runner_context_id: "run-batch-context".into(),
                stream_id: "tool-1:0".into(),
                succeeded: true,
                exit_code: Some(0),
                timed_out: false,
                duration_ms: 1_000,
                result: None,
            },
        )
        .await;
    assert!(matches!(
        detached::live_progress(&orch, &request_id),
        Some(BatchProgress::Running { index: 2, .. })
    ));
    orch.fleet
        .handle_runner_callback(
            &orch,
            RunnerCallback::ProcessItemCompleted {
                runner_context_id: "run-batch-context".into(),
                stream_id: "tool-1:2".into(),
                succeeded: true,
                exit_code: Some(0),
                timed_out: false,
                duration_ms: 1_000,
                result: None,
            },
        )
        .await;
    assert!(matches!(
        detached::live_progress(&orch, &request_id),
        Some(BatchProgress::Placed { .. })
    ));
    live.begin_settling_for_test();
    assert_eq!(
        detached::live_progress(&orch, &request_id),
        Some(BatchProgress::Settling)
    );
    drop(live);
    assert_eq!(detached::live_progress(&orch, &request_id), None);
}

#[test]
fn mcp_relay_authorization_binds_context_executor_generation_and_run() {
    let pool = Fleet::default();
    insert_bound_relay_context(&pool);

    assert!(pool
        .authorize_mcp_relay("executor-1", 7, "context", &relay_request(Some("run-1")))
        .is_ok());
    for (executor, generation, context, run_id) in [
        ("executor-2", 7, "context", Some("run-1")),
        ("executor-1", 8, "context", Some("run-1")),
        ("executor-1", 7, "unknown", Some("run-1")),
        ("executor-1", 7, "context", Some("run-2")),
        ("executor-1", 7, "context", None),
    ] {
        assert!(pool
            .authorize_mcp_relay(executor, generation, context, &relay_request(run_id))
            .is_err());
    }

    let mut foreign_thread = relay_request(Some("run-1"));
    foreign_thread.thread_id = Some("thread-owned-by-another-run".into());
    assert!(pool
        .authorize_mcp_relay("executor-1", 7, "context", &foreign_thread)
        .unwrap_err()
        .contains("thread identity"));
}

fn announced(
    runner_context_id: &str,
    request_id: &str,
    attempt_id: &str,
) -> cairn_common::executor_protocol::RelayIdentity {
    cairn_common::executor_protocol::RelayIdentity::Batch {
        runner_context_id: runner_context_id.into(),
        request_id: request_id.into(),
        attempt_id: attempt_id.into(),
    }
}

fn authorize(pool: &Fleet, generation: u64) -> Result<(), String> {
    pool.authorize_mcp_relay(
        "executor-1",
        generation,
        "context",
        &relay_request(Some("run-1")),
    )
}

/// A link gap detaches a batch's relay rather than ending it: calls on either
/// link are refused naming the gap, output keeps being accepted, and the
/// reattach that names the batch for the same attempt binds it to the new
/// link (cairn/5631).
#[tokio::test]
async fn a_link_gap_detaches_a_batch_relay_and_its_reannouncement_rebinds_it() {
    let dir = tempfile::tempdir().unwrap();
    let orch = test_orchestrator(dir.path()).await;
    let pool = &orch.fleet;
    insert_bound_relay_context(pool);

    pool.relay_link_closed("executor-1", 7);
    assert!(pool.holds_runner_context("context"));
    for generation in [7, 8] {
        let refused = authorize(pool, generation).unwrap_err();
        assert!(
            refused.starts_with("relay detached by a link loss at ")
                && refused.ends_with("; the executor has not reattached"),
            "{refused}"
        );
    }
    assert_eq!(
        pool.handle_runner_callback(
            &orch,
            RunnerCallback::ProcessEvent {
                runner_context_id: "context".into(),
                stream_id: "tool-1:0".into(),
                payload: "still running".into(),
            },
        )
        .await,
        RunnerCallbackResult::Completed,
        "a detached batch's output is still taken"
    );

    pool.reattach_relays(
        "executor-1",
        8,
        &[announced("context", "request-1", "attempt-1")],
    );
    assert!(authorize(pool, 8).is_ok());
    assert!(
        authorize(pool, 7).is_err(),
        "the old link is not the batch's"
    );

    // A late close of the link it has left does not detach it again.
    pool.relay_link_closed("executor-1", 7);
    assert!(authorize(pool, 8).is_ok());

    // Its end is remembered, and a call after it is refused by that end.
    pool.settle_runner_contexts_for_request("request-1", "attempt-1");
    let refused = authorize(pool, 8).unwrap_err();
    assert!(
        refused.starts_with("the batch that held this relay settled at "),
        "{refused}"
    );
}

/// An overlapping redial: the new link reattaches before the old one's close
/// is seen. The batch is rebound straight from the old link, and that close,
/// arriving late, leaves it bound.
#[test]
fn an_overlapping_redial_rebinds_a_batch_still_bound_to_the_old_link() {
    let pool = Fleet::default();
    insert_bound_relay_context(&pool);
    pool.reattach_relays(
        "executor-1",
        8,
        &[announced("context", "request-1", "attempt-1")],
    );
    pool.relay_link_closed("executor-1", 7);
    assert!(authorize(&pool, 8).is_ok());
}

/// A superseded link's announcement, arriving after the new link's, changes
/// nothing: neither one naming the batch (which would bind it back to the dead
/// link) nor one omitting it (which would revoke a live batch).
#[test]
fn a_late_announcement_from_a_superseded_link_neither_rebinds_nor_revokes() {
    let pool = Fleet::default();
    let (old_tx, _old_rx) = mpsc::unbounded_channel();
    let old = pool.attach_executor(old_tx);
    let authorize_at = |generation| {
        pool.authorize_mcp_relay(
            COLOCATED_EXECUTOR_ID,
            generation,
            "context",
            &relay_request(Some("run-1")),
        )
    };
    insert_bound_relay_context(&pool);
    {
        let mut contexts = pool.runner_contexts.lock().unwrap();
        let binding = contexts
            .get_mut("context")
            .unwrap()
            .context
            .executor_binding
            .as_mut()
            .unwrap();
        binding.executor_id = COLOCATED_EXECUTOR_ID.into();
        binding.link = BatchLink::Bound(old);
    }
    let (new_tx, _new_rx) = mpsc::unbounded_channel();
    let new = pool.attach_executor(new_tx);
    assert!(new > old);
    let batch = announced("context", "request-1", "attempt-1");
    pool.reattach_relays(COLOCATED_EXECUTOR_ID, new, std::slice::from_ref(&batch));
    assert!(authorize_at(new).is_ok());

    pool.reattach_relays(COLOCATED_EXECUTOR_ID, old, std::slice::from_ref(&batch));
    assert!(authorize_at(new).is_ok(), "a stale naming does not rebind");
    pool.reattach_relays(COLOCATED_EXECUTOR_ID, old, &[]);
    assert!(
        pool.holds_runner_context("context"),
        "a stale omission does not revoke"
    );
    assert!(authorize_at(new).is_ok());
}

/// With no connection record to say which link is current, a batch's binding
/// still never moves to an older link, and an older link's omission does not
/// revoke a batch bound to a newer one.
#[test]
fn a_batch_binding_never_moves_to_an_older_link() {
    let pool = Fleet::default();
    insert_bound_relay_context(&pool);
    pool.reattach_relays(
        "executor-1",
        9,
        &[announced("context", "request-1", "attempt-1")],
    );
    pool.relay_link_closed("executor-1", 9);
    pool.reattach_relays(
        "executor-1",
        8,
        &[announced("context", "request-1", "attempt-1")],
    );
    pool.reattach_relays("executor-1", 8, &[]);
    assert!(pool.holds_runner_context("context"));
    assert!(authorize(&pool, 8)
        .unwrap_err()
        .starts_with("relay detached"));
    pool.reattach_relays(
        "executor-1",
        10,
        &[announced("context", "request-1", "attempt-1")],
    );
    assert!(authorize(&pool, 10).is_ok());
}

/// A reattach that names the batch for another attempt, names another
/// context, or comes from another executor does not rebind it; once its own
/// executor reattaches without it, it is revoked, and a call on it says so.
#[test]
fn a_batch_relay_not_reannounced_for_its_attempt_is_revoked_by_name() {
    for (executor, announcement) in [
        ("executor-1", announced("context", "request-2", "attempt-1")),
        ("executor-1", announced("context", "request-1", "attempt-2")),
        ("executor-1", announced("other", "request-1", "attempt-1")),
        ("executor-2", announced("context", "request-1", "attempt-1")),
    ] {
        let pool = Fleet::default();
        insert_bound_relay_context(&pool);
        pool.relay_link_closed("executor-1", 7);
        pool.reattach_relays(executor, 8, std::slice::from_ref(&announcement));
        if executor == "executor-2" {
            assert!(
                pool.holds_runner_context("context"),
                "another executor's reattach says nothing about this batch"
            );
            pool.reattach_relays("executor-1", 8, &[]);
        }
        assert!(!pool.holds_runner_context("context"), "{announcement:?}");
        let refused = authorize(&pool, 8).unwrap_err();
        assert!(
            refused.starts_with("relay revoked: executor executor-1 reattached at ")
                && refused.ends_with(" (link 8) without announcing this batch"),
            "{refused}"
        );
    }
}

/// A batch dispatched on the new link after the executor composed its
/// announcement is bound to that link already, and is kept.
#[test]
fn a_batch_bound_to_the_reattaching_link_is_kept_unannounced() {
    let pool = Fleet::default();
    insert_bound_relay_context(&pool);
    pool.runner_contexts
        .lock()
        .unwrap()
        .get_mut("context")
        .unwrap()
        .context
        .executor_binding
        .as_mut()
        .unwrap()
        .link = BatchLink::Bound(8);
    pool.reattach_relays("executor-1", 8, &[]);
    assert!(authorize(&pool, 8).is_ok());
}

/// The ledger that words a late refusal holds digests, never the capability
/// itself, and a context it never saw is named as unknown.
#[test]
fn the_closed_context_ledger_keys_by_digest_only() {
    let pool = Fleet::default();
    insert_bound_relay_context(&pool);
    pool.cancel_runner_contexts_for_request("request-1", "attempt-1");
    let keys = pool.closed_runner_context_keys();
    assert_eq!(
        keys,
        [relay_authority::ClosedRunnerContexts::key("context")]
    );
    assert!(keys.iter().all(|key| !key
        .windows("context".len())
        .any(|window| window == b"context")));
    assert!(authorize(&pool, 7)
        .unwrap_err()
        .starts_with("the batch that held this relay was cancelled at "));
    assert!(pool
        .authorize_mcp_relay(
            "executor-1",
            7,
            "never-issued",
            &relay_request(Some("run-1"))
        )
        .unwrap_err()
        .starts_with("unknown runner callback context: not issued by this runner process"));
}

#[test]
fn terminal_result_revocation_prevents_replay() {
    let pool = Fleet::default();
    insert_bound_relay_context(&pool);
    pool.settle_runner_contexts_for_request("request-1", "attempt-1");

    assert!(pool
        .authorize_mcp_relay("executor-1", 7, "context", &relay_request(Some("run-1")))
        .is_err());
}

/// The acceptance case for the whole issue: a command that outlives the
/// link that ordered it is delivered to whoever asked for it.
///
/// The executor keeps its cells running across a link gap, so the result of
/// a check that was sixteen minutes into its run when the socket reset is
/// still coming. It arrives on the connection that came back, under a new
/// generation, and the waiter it belongs to has to still be there.
#[tokio::test]
async fn work_running_on_a_machine_outlives_the_link_that_ordered_it() {
    let pool = Fleet::default();
    let (sender, _executor) = mpsc::unbounded_channel();
    let generation = pool.attach_executor(sender);
    let key = ("request".to_string(), "attempt".to_string());
    let result_rx = claim_entry(&pool, &key, COLOCATED_EXECUTOR_ID, generation, Some("job"));

    // Nobody decided this. A socket reset is not a statement about the
    // process on the other end of it.
    assert!(pool.disconnect_advertised_executor(COLOCATED_EXECUTOR_ID, generation));
    assert!(
        pool.queue.holds(&key),
        "a link that went away on its own must not resolve the work it was carrying"
    );

    let (sender, _executor) = mpsc::unbounded_channel();
    let reattached = pool.attach_executor(sender);
    assert_ne!(reattached, generation);

    let finished = CellOutcome::Cancelled {
        request_id: key.0.clone(),
        attempt_id: key.1.clone(),
    };
    pool.handle_executor_message(
        COLOCATED_EXECUTOR_ID,
        reattached,
        ExecutorMessage::Result {
            request_id: key.0.clone(),
            attempt_id: key.1.clone(),
            outcome: finished.clone(),
        },
    );
    assert_eq!(
        result_rx.await.unwrap(),
        finished,
        "the result computed across the gap must reach the waiter that asked for it"
    );
}

/// The machine's own account of what it is running is what keeps held work
/// held, and the result then arrives on the new connection like any other.
#[tokio::test]
async fn work_a_reattached_machine_says_it_still_has_stays_held() {
    let pool = Fleet::default();
    let (sender, _executor) = mpsc::unbounded_channel();
    let generation = pool.attach_executor(sender);
    let key = ("request".to_string(), "attempt".to_string());
    let result_rx = claim_entry(&pool, &key, COLOCATED_EXECUTOR_ID, generation, None);
    assert!(pool.disconnect_advertised_executor(COLOCATED_EXECUTOR_ID, generation));

    let (sender, _executor) = mpsc::unbounded_channel();
    let reattached = pool.attach_executor(sender);
    let executing: cairn_common::executor_protocol::ExecutingCellRequest =
        serde_json::from_value(serde_json::json!({
            "requestId": key.0,
            "attemptId": key.1,
            "startedAtUnixMs": 0,
            "processIds": [],
        }))
        .unwrap();
    pool.handle_executor_message(
        COLOCATED_EXECUTOR_ID,
        reattached,
        ExecutorMessage::SnapshotUpdated {
            snapshot: FleetSnapshot {
                executing_requests: vec![executing],
                ..FleetSnapshot::default()
            },
            health: ExecutorSubstrateReport::default(),
        },
    );
    assert!(
        pool.queue.holds(&key),
        "work the machine says it is still running stays held"
    );

    let finished = CellOutcome::Cancelled {
        request_id: key.0.clone(),
        attempt_id: key.1.clone(),
    };
    pool.handle_executor_message(
        COLOCATED_EXECUTOR_ID,
        reattached,
        ExecutorMessage::Result {
            request_id: key.0.clone(),
            attempt_id: key.1.clone(),
            outcome: finished.clone(),
        },
    );
    assert_eq!(result_rx.await.unwrap(), finished);
}

/// Holding work is not hoping for it. A machine that comes back without the
/// work says so in the first snapshot of the new connection, and that is the
/// answer — waiting out a watchdog for a result nobody is going to send is
/// the other way to lose a lane.
#[tokio::test]
async fn a_machine_that_reattaches_without_the_work_resolves_it_at_once() {
    let pool = Fleet::default();
    let (sender, _executor) = mpsc::unbounded_channel();
    let generation = pool.attach_executor(sender);
    let key = ("request".to_string(), "attempt".to_string());
    let result_rx = claim_entry(&pool, &key, COLOCATED_EXECUTOR_ID, generation, None);
    assert!(pool.disconnect_advertised_executor(COLOCATED_EXECUTOR_ID, generation));

    let (sender, _executor) = mpsc::unbounded_channel();
    let reattached = pool.attach_executor(sender);

    // A first snapshot without it means the process behind the link was
    // replaced while the link was down. Applied through the ingest the
    // transport uses, since that is where this accounting was once skipped.
    pool.apply_executor_snapshot(
        COLOCATED_EXECUTOR_ID,
        reattached,
        FleetSnapshot::default(),
        ExecutorSubstrateReport::default(),
    );
    let outcome = result_rx.await.unwrap();
    assert!(
        matches!(
            &outcome,
            CellOutcome::Unavailable {
                reason: CellUnavailableReason::ExecutorUnavailable,
                diagnostic,
            } if diagnostic.contains("no longer running or queued")
                && diagnostic.contains(&format!("reattached at generation {reattached}"))
                && diagnostic.contains(&format!("running at generation {generation}"))
        ),
        "the settlement names the generation that lost the work: {outcome:?}"
    );
}

#[tokio::test]
async fn a_witnessed_grant_disappearing_on_the_same_connection_settles_its_waiter() {
    let pool = Fleet::default();
    let (sender, _executor) = mpsc::unbounded_channel();
    let generation = pool.attach_executor(sender);
    let key = ("request".to_string(), "attempt".to_string());
    let mut result_rx = claim_entry(&pool, &key, COLOCATED_EXECUTOR_ID, generation, None);
    let apply = |snapshot| {
        pool.apply_executor_snapshot(
            COLOCATED_EXECUTOR_ID,
            generation,
            snapshot,
            ExecutorSubstrateReport::default(),
        );
    };
    apply(FleetSnapshot::default());
    assert!(matches!(
        result_rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    apply(FleetSnapshot {
        granted_attempts: vec![cairn_common::executor_protocol::GrantedAttempt {
            request_id: key.0.clone(),
            attempt_id: key.1.clone(),
        }],
        ..FleetSnapshot::default()
    });
    assert!(matches!(
        result_rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    apply(FleetSnapshot::default());
    assert!(matches!(
        result_rx.await.unwrap(),
        CellOutcome::Unavailable {
            reason: CellUnavailableReason::ExecutorUnavailable,
            ref diagnostic,
        } if diagnostic.contains("no longer lists witnessed request")
    ));
    assert!(!pool.queue.holds(&key));
}

/// Granted work that has not spawned a process yet (acquiring its cell,
/// materializing its checkout, between batch items) is listed only as a
/// granted attempt. That is account enough: a link blip in one of those
/// phases must not resolve live work as lost.
#[tokio::test]
async fn granted_work_without_a_process_yet_stays_held_across_a_reattach() {
    let pool = Fleet::default();
    let (sender, _executor) = mpsc::unbounded_channel();
    let generation = pool.attach_executor(sender);
    let key = ("request".to_string(), "attempt".to_string());
    let _result_rx = claim_entry(&pool, &key, COLOCATED_EXECUTOR_ID, generation, None);
    assert!(pool.disconnect_advertised_executor(COLOCATED_EXECUTOR_ID, generation));

    let (sender, _executor) = mpsc::unbounded_channel();
    let reattached = pool.attach_executor(sender);
    pool.apply_executor_snapshot(
        COLOCATED_EXECUTOR_ID,
        reattached,
        FleetSnapshot {
            granted_attempts: vec![cairn_common::executor_protocol::GrantedAttempt {
                request_id: key.0.clone(),
                attempt_id: key.1.clone(),
            }],
            ..FleetSnapshot::default()
        },
        ExecutorSubstrateReport::default(),
    );
    assert!(
        pool.queue.holds(&key),
        "a granted attempt the machine still holds stays held"
    );
}

/// The supervisor is the only thing that knows a machine's process is gone,
/// because killing it is what the supervisor does. Its verdict resolves held
/// work immediately rather than leaving it to a clock.
#[tokio::test]
async fn the_supervisors_verdict_that_a_machine_is_gone_resolves_its_held_work() {
    let pool = Fleet::default();
    let (sender, _executor) = mpsc::unbounded_channel();
    let generation = pool.attach_executor(sender);
    let key = ("request".to_string(), "attempt".to_string());
    let result_rx = claim_entry(&pool, &key, COLOCATED_EXECUTOR_ID, generation, None);
    assert!(pool.disconnect_advertised_executor(COLOCATED_EXECUTOR_ID, generation));

    pool.abandon_held_executions(
        COLOCATED_EXECUTOR_ID,
        "the runner recycled this machine's executor: telemetry stalled",
    );
    assert!(matches!(
        result_rx.await.unwrap(),
        CellOutcome::Unavailable {
            reason: CellUnavailableReason::ExecutorUnavailable,
            ref diagnostic,
        } if diagnostic.contains("telemetry stalled")
    ));
    assert!(pool.queue.len() == 0);
}

/// Dial `executor_id` in as a remote, as the transport does on every Hello.
fn dial_remote(pool: &Fleet, executor_id: &str) -> u64 {
    let (_, connection) = fleet_entry(executor_id, "linux", 0, &[]);
    pool.attach_advertised_executor(
        Arc::unwrap_or_clone(connection.advertisement),
        Default::default(),
        connection.sender,
        false,
        None,
        None,
    )
    .expect("attach")
}

/// The 10-19 specimen: the runner slept, each remote's kernel gave up on
/// its link and the executor redialled, and the runner, which never saw a
/// close, still held the old connection when the new one arrived. That is
/// a link gap whose close went unseen, and the work comes through it.
#[tokio::test]
async fn a_redial_that_replaces_an_unclosed_connection_holds_its_work() {
    let pool = Fleet::default();
    let first = dial_remote(&pool, "fedora");
    let key = ("request".to_string(), "attempt".to_string());
    let result_rx = claim_entry(&pool, &key, "fedora", first, None);
    let offered = ("offered".to_string(), "attempt".to_string());
    let (offered_entry, _offered_rx) = enqueue_entry(&pool, &offered, None);
    assert!(pool
        .queue
        .offer(&offered_entry, "fedora", "fedora", first, unix_time_ms()));

    let second = dial_remote(&pool, "fedora");
    assert!(second > first);
    assert!(
        pool.queue.holds(&key),
        "a replaced connection is not a dead process"
    );
    assert_eq!(
        pool.queue.binding_of(&key),
        Some(("fedora".to_string(), second)),
        "the claim is carried onto the connection that replaced its own"
    );
    assert!(
        matches!(
            pool.queue.state_of(&offered_entry),
            Some(queue::EntryState::Queued)
        ),
        "an entry that was only offered goes back to the queue"
    );

    pool.apply_executor_snapshot(
        "fedora",
        second,
        FleetSnapshot {
            granted_attempts: vec![cairn_common::executor_protocol::GrantedAttempt {
                request_id: key.0.clone(),
                attempt_id: key.1.clone(),
            }],
            ..FleetSnapshot::default()
        },
        ExecutorSubstrateReport::default(),
    );
    assert!(
        pool.queue.holds(&key),
        "work the machine says it still holds stays held"
    );

    let finished = CellOutcome::Cancelled {
        request_id: key.0.clone(),
        attempt_id: key.1.clone(),
    };
    pool.handle_executor_message(
        "fedora",
        second,
        ExecutorMessage::Result {
            request_id: key.0.clone(),
            attempt_id: key.1.clone(),
            outcome: finished.clone(),
        },
    );
    assert_eq!(result_rx.await.unwrap(), finished);
}

/// The transport drains a closed link's ingest before it ends the
/// generation, so a result read off that socket is settled. A redial that
/// replaces an unclosed link cannot wait for that drain: the claim is
/// rebound first, and a result the runner already received on the old link
/// arrives stamped with the old generation. The executor no longer lists
/// the attempt as granted, so no second result is coming, and dropping
/// this one would leave the waiter to the end-to-end watchdog.
#[tokio::test]
async fn a_result_read_off_the_replaced_link_settles_the_rebound_claim() {
    let pool = Fleet::default();
    let first = dial_remote(&pool, "fedora");
    let key = ("request".to_string(), "attempt".to_string());
    let result_rx = claim_entry(&pool, &key, "fedora", first, None);

    let second = dial_remote(&pool, "fedora");
    assert_eq!(
        pool.queue.binding_of(&key),
        Some(("fedora".to_string(), second))
    );

    let ran_on_the_old_link = CellOutcome::Completed {
        request_id: key.0.clone(),
        attempt_id: key.1.clone(),
        exit_code: Some(0),
        output: "ran".into(),
        timed_out: false,
        metadata: CellExecutionMeta {
            commands_exited_at_unix_ms: None,
            warmth: None,
            cell_origin: None,
            load_context: None,
            executor_id: "fedora".into(),
            executor_device_id: "device".into(),
            executor_connection_generation: first,
            cell_id: "cell".into(),
            cell_epoch: 1,
            started_at_unix_ms: 1,
            finished_at_unix_ms: 2,
            duration_ms: None,
            peak_rss_bytes: None,
            peak_physical_footprint_bytes: None,
            peak_process: None,
            cpu: None,
            allocation: None,
            disk_delta_bytes: None,
            measurement_quality: None,
            environment_fingerprint: String::new(),
            verdict_platform: None,
            verdict_arch: None,
            verdict_environment_hash: None,
            sandbox: None,
            toolchain_fingerprint: None,
            containment: None,
            stop: None,
            test_phase: None,
            build_job_processes: None,
        },
        mutation_delta: None,
        sandbox_denials: Vec::new(),
        tracked_modifications: None,
        node_scratch: None,
    };
    pool.handle_executor_message(
        "fedora",
        first,
        ExecutorMessage::Result {
            request_id: key.0.clone(),
            attempt_id: key.1.clone(),
            outcome: ran_on_the_old_link,
        },
    );

    match result_rx.await.unwrap() {
        CellOutcome::Completed {
            exit_code,
            output,
            metadata,
            ..
        } => {
            assert_eq!(exit_code, Some(0));
            assert_eq!(output, "ran");
            assert_eq!(
                metadata.executor_connection_generation, second,
                "the result is recorded against the link that carries the claim"
            );
        }
        other => panic!("the real result reaches the waiter, got {other:?}"),
    }
    assert!(!pool.queue.holds(&key));
}

/// Accepting a result from an older link is about the same machine's claim
/// carried forward, never about another machine's claim.
#[tokio::test]
async fn a_result_from_another_machine_does_not_settle_the_claim() {
    let pool = Fleet::default();
    let fedora = dial_remote(&pool, "fedora");
    let bbs = dial_remote(&pool, "bbs");
    let key = ("request".to_string(), "attempt".to_string());
    let _result_rx = claim_entry(&pool, &key, "bbs", bbs, None);
    let late = CellOutcome::Cancelled {
        request_id: key.0.clone(),
        attempt_id: key.1.clone(),
    };
    pool.handle_executor_message(
        "fedora",
        fedora,
        ExecutorMessage::Result {
            request_id: key.0.clone(),
            attempt_id: key.1.clone(),
            outcome: late,
        },
    );
    assert_eq!(
        pool.queue.binding_of(&key),
        Some(("bbs".to_string(), bbs)),
        "another machine's answer does not settle this claim"
    );
}

/// The same redial from a process that restarted while the link was down.
/// Its opening snapshot lists none of the old work, and that is what
/// settles it, naming the generation that lost it.
#[tokio::test]
async fn a_replacing_redial_from_a_restarted_process_settles_its_work_on_the_first_snapshot() {
    let pool = Fleet::default();
    let first = dial_remote(&pool, "fedora");
    let key = ("request".to_string(), "attempt".to_string());
    let result_rx = claim_entry(&pool, &key, "fedora", first, None);

    let second = dial_remote(&pool, "fedora");
    pool.apply_executor_snapshot(
        "fedora",
        second,
        FleetSnapshot::default(),
        ExecutorSubstrateReport::default(),
    );
    let outcome = result_rx.await.unwrap();
    assert!(
        matches!(
            &outcome,
            CellOutcome::Unavailable {
                reason: CellUnavailableReason::ExecutorUnavailable,
                diagnostic,
            } if diagnostic.contains("no longer running or queued")
                && diagnostic.contains(&format!("running at generation {first}"))
                && !diagnostic.contains("replaced before returning")
        ),
        "{outcome:?}"
    );
}

/// The old socket does close eventually, and by then it is not the
/// machine's link. Its close must not reach the work it handed on.
#[tokio::test]
async fn the_replaced_generations_late_close_leaves_the_rebound_work_alone() {
    let pool = Fleet::default();
    let first = dial_remote(&pool, "fedora");
    let key = ("request".to_string(), "attempt".to_string());
    let _result_rx = claim_entry(&pool, &key, "fedora", first, None);
    let second = dial_remote(&pool, "fedora");

    assert!(!pool.disconnect_advertised_executor("fedora", first));
    assert!(pool.queue.holds(&key));
    assert_eq!(
        pool.queue.binding_of(&key),
        Some(("fedora".to_string(), second))
    );
    assert_eq!(
        pool.lock_connections().get("fedora").unwrap().generation,
        second
    );
}

/// The colocated machine is the one case where the runner knows a
/// replacement is a new process: its supervisor spawned it.
#[tokio::test]
async fn a_colocated_replace_still_fails_its_work() {
    let pool = Fleet::default();
    let (sender, _executor) = mpsc::unbounded_channel();
    let generation = pool.attach_executor(sender);
    let key = ("request".to_string(), "attempt".to_string());
    let result_rx = claim_entry(&pool, &key, COLOCATED_EXECUTOR_ID, generation, None);

    let (sender, _replacement) = mpsc::unbounded_channel();
    pool.attach_executor(sender);
    assert!(matches!(
        result_rx.await.unwrap(),
        CellOutcome::Unavailable {
            reason: CellUnavailableReason::ExecutorUnavailable,
            ref diagnostic,
        } if diagnostic.contains("replaced before returning a result")
    ));
}

/// Recycling the colocated executor is a fact about one machine. The
/// remotes keep their links and their work, and the listener every remote
/// dials is not restaged by it.
#[tokio::test]
async fn resetting_the_colocated_link_leaves_remote_links_untouched() {
    let pool = Fleet::default();
    let (sender, _executor) = mpsc::unbounded_channel();
    let colocated = pool.attach_executor(sender);
    let local_key = ("local".to_string(), "attempt".to_string());
    let local_rx = claim_entry(&pool, &local_key, COLOCATED_EXECUTOR_ID, colocated, None);
    let remotes: Vec<_> = ["fedora", "bbs"]
        .into_iter()
        .map(|id| {
            let generation = dial_remote(&pool, id);
            let key = (format!("{id}-request"), "attempt".to_string());
            let rx = claim_entry(&pool, &key, id, generation, None);
            (id, generation, key, rx)
        })
        .collect();
    let mut listener = pool.subscribe_executor_listener();
    listener.borrow_and_update();

    let abandonment = pool.abandon_stalled_colocated_link(
        COLOCATED_EXECUTOR_ID,
        colocated,
        LinkStallCause::TelemetryFrozen {
            frozen_ms: EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS + 1,
        },
    );
    assert!(abandonment.disconnected);
    assert!(matches!(
        local_rx.await.unwrap(),
        CellOutcome::Unavailable { .. }
    ));
    for (id, generation, key, _rx) in &remotes {
        assert_eq!(
            pool.lock_connections().get(*id).unwrap().generation,
            *generation
        );
        assert!(pool.queue.holds(key), "{id} keeps its claim");
        assert_eq!(
            pool.queue.binding_of(key),
            Some((id.to_string(), *generation))
        );
    }
    assert!(
        !listener.has_changed().unwrap(),
        "a colocated reset does not restage the listener"
    );
}

/// The runner's own shutdown answers a claim on its colocated machine at
/// once, and says what happened: the runner stopped. Answering it as a
/// closed connection is what recorded a restart as the machine's fault.
#[tokio::test]
async fn runner_shutdown_answers_a_colocated_claim_with_the_stop() {
    let pool = Fleet::default();
    let (sender, mut executor) = mpsc::unbounded_channel();
    let generation = pool.attach_executor(sender);
    assert!(matches!(
        executor.recv().await,
        Some(ExecutorMessage::DrainModeRequest { enabled: false, .. })
    ));
    let key = ("request".to_string(), "attempt".to_string());
    let result_rx = claim_entry(&pool, &key, COLOCATED_EXECUTOR_ID, generation, Some("job"));

    let started = std::time::Instant::now();
    assert!(pool.begin_runner_shutdown());
    assert!(started.elapsed() < Duration::from_millis(100));
    assert!(matches!(
        executor.recv().await,
        Some(ExecutorMessage::Shutdown { .. })
    ));
    assert!(matches!(
        result_rx.await.unwrap(),
        CellOutcome::Unavailable {
            reason: CellUnavailableReason::RunnerStopping { .. },
            ref diagnostic,
        } if diagnostic.contains("began shutting down")
    ));
    assert_eq!(
        pool.take_disconnect_origin(COLOCATED_EXECUTOR_ID, generation),
        Some(ExecutorDisconnectOrigin::RunnerInitiated)
    );
}

#[test]
fn check_batch_populates_the_existing_learned_estimate_field() {
    let aggregate = aggregate_batch_learned_estimates(&[Some(
        cairn_common::executor_protocol::LearnedResourceEstimate {
            sample_count: 2,
            upper_duration_ms: Some(600),
            upper_peak_rss_bytes: Some(400),
            upper_disk_growth_bytes: None,
        },
    )]);
    let estimate = aggregate.expect("a profiled check must produce a snapshot estimate");
    assert_eq!(estimate.sample_count, 2);
    assert_eq!(estimate.upper_duration_ms, Some(600));
    assert_eq!(estimate.upper_peak_rss_bytes, Some(400));
    assert_eq!(estimate.upper_disk_growth_bytes, None);
}

#[test]
fn check_batch_with_partial_profile_coverage_has_no_estimate() {
    let aggregate = aggregate_batch_learned_estimates(&[
        Some(cairn_common::executor_protocol::LearnedResourceEstimate {
            sample_count: 2,
            upper_duration_ms: Some(600),
            upper_peak_rss_bytes: Some(400),
            upper_disk_growth_bytes: None,
        }),
        None,
    ]);
    assert_eq!(aggregate, None);
}

async fn test_orchestrator(config_dir: &Path) -> Orchestrator {
    let local = LocalDb::open_migrated_for_test(config_dir.join("build-slots.db"))
        .await
        .unwrap();
    let search = Arc::new(SearchIndex::open_or_create(config_dir.join("search")).unwrap());
    Orchestrator::builder(
        Arc::new(DbState::new(Arc::new(local), search)),
        Arc::new(TestServicesBuilder::new().build()),
        config_dir.to_path_buf(),
    )
    .build()
}

fn cache_batch_item(env: Vec<(String, String)>) -> ProcessBatchItem {
    ProcessBatchItem {
        header: "item".into(),
        stream_id: "stream:0".into(),
        execution: ProcessBatchExecution::NativeShell,
        program: String::new(),
        args: vec!["cargo build".into()],
        env,
        stdin: None,
        timeout_ms: 1_000,
        command_resource_identity: None,
        verdict_environment_names: Vec::new(),
        silence_bound_ms: 0,
    }
}

/// The request's env reaches every command in the batch.
///
/// This is the second half of the run-identity seam: `placed_process_env`
/// composes what a batch carries, and this is where that vector becomes the
/// env of each process the executor spawns. A composer that states the run
/// and a serializer that drops it would leave every agent shell anonymous
/// while both halves looked correct in isolation (CAIRN-3381), so assert the
/// crossing rather than either side of it.
#[test]
fn a_placed_batch_hands_its_env_to_every_command_it_carries() {
    let env = vec![
        ("CAIRN_RUN_ID".to_string(), "run-7".to_string()),
        (
            "CAIRN_WORKTREE_BRANCH".to_string(),
            "agent/cairn-3381-builder-0".to_string(),
        ),
    ];
    let batch = serialize_process_batch(
        ResolvedRunBatch {
            request: Default::default(),
            run_context: None,
            resolved: vec![
                (
                    "cairn check run rust-fmt".to_string(),
                    Ok(RunSpec::Shell {
                        command: "cairn check run rust-fmt".to_string(),
                        timeout: None,
                        args: Vec::new(),
                    }),
                ),
                (
                    "cairn read cairn:~/plan".to_string(),
                    Ok(RunSpec::Shell {
                        command: "cairn read cairn:~/plan".to_string(),
                        timeout: None,
                        args: Vec::new(),
                    }),
                ),
            ],
            tool_use_id: "tool-use".to_string(),
            stop_on_error: true,
            originally_sequential: true,
            execution_residency: None,
            node_scratch: None,
        },
        &env,
        "runner-context".to_string(),
        ProcessSandboxMode::Unconfined,
        None,
    )
    .expect("a shell batch serializes");

    assert_eq!(batch.items.len(), 2);
    for item in &batch.items {
        assert_eq!(
            item.env, env,
            "every command in a placed batch runs with the batch's env"
        );
    }
}

/// The SDK travels with a batch that can run JavaScript and with no other,
/// so shell, Python, and MATLAB work is not taxed with bytes it never reads
/// -- and so neither way of reaching bun is left out.
#[test]
fn a_placed_shell_batch_preserves_procedure_arguments() {
    let batch = serialize_process_batch(
        ResolvedRunBatch {
            request: Default::default(),
            run_context: None,
            resolved: vec![(
                "procedure".to_string(),
                Ok(RunSpec::Shell {
                    command: "printf '%s' \"$@\"".to_string(),
                    timeout: None,
                    args: vec!["first".to_string(), "two words".to_string()],
                }),
            )],
            tool_use_id: "tool-use".to_string(),
            stop_on_error: true,
            originally_sequential: true,
            execution_residency: None,
            node_scratch: None,
        },
        &[],
        "runner-context".to_string(),
        ProcessSandboxMode::Unconfined,
        None,
    )
    .unwrap();

    assert_eq!(
        batch.items[0].args,
        ["printf '%s' \"$@\"", "first", "two words"]
    );
}

/// A launcher-wrapped command reaches the executor byte for byte, so the
/// shell the agent named is the one that runs.
#[test]
fn a_placed_shell_batch_carries_a_wrapped_command_as_written() {
    let command = "bash -lc 'set -e; echo \"$0\"' && echo 'b   c'";
    let batch = serialize_process_batch(
        ResolvedRunBatch {
            request: Default::default(),
            run_context: None,
            resolved: vec![(
                command.to_string(),
                Ok(RunSpec::Shell {
                    command: command.to_string(),
                    timeout: None,
                    args: Vec::new(),
                }),
            )],
            tool_use_id: "tool-use".to_string(),
            stop_on_error: true,
            originally_sequential: true,
            execution_residency: None,
            node_scratch: None,
        },
        &[],
        "runner-context".to_string(),
        ProcessSandboxMode::Unconfined,
        None,
    )
    .unwrap();

    assert_eq!(batch.items[0].header, command);
    assert_eq!(batch.items[0].args, [command]);
}

#[test]
fn the_sdk_travels_with_javascript_batches_by_either_route() {
    let item = |execution, program: &str, args: Vec<&str>| ProcessBatchItem {
        header: "item".into(),
        stream_id: "item".into(),
        execution,
        program: program.into(),
        args: args.into_iter().map(String::from).collect(),
        env: Vec::new(),
        stdin: None,
        timeout_ms: 1_000,
        command_resource_identity: None,
        verdict_environment_names: Vec::new(),
        silence_bound_ms: 0,
    };

    // An inline TypeScript item resolves to a direct `bun` program.
    assert!(batch_runs_javascript(&[item(
        ProcessBatchExecution::Direct,
        "bun",
        vec!["-e", "import {read} from '@cairn/sdk'"]
    )]));
    // A shell item carries its command line in `args` instead.
    assert!(batch_runs_javascript(&[item(
        ProcessBatchExecution::NativeShell,
        "",
        vec!["bun run scripts/report.ts"]
    )]));
    assert!(batch_runs_javascript(&[item(
        ProcessBatchExecution::NativeShell,
        "",
        vec!["bunx --bun vite build"]
    )]));
    // A batch with no JavaScript in it carries nothing.
    assert!(!batch_runs_javascript(&[
        item(
            ProcessBatchExecution::Direct,
            "python3",
            vec!["-c", "print(1)"]
        ),
        item(
            ProcessBatchExecution::NativeShell,
            "",
            vec!["cargo test --workspace"]
        ),
    ]));
    // A program whose name merely CONTAINS a runtime name is not that
    // runtime, so the direct match is exact rather than a substring.
    assert!(!batch_runs_javascript(&[item(
        ProcessBatchExecution::Direct,
        "nodemon",
        Vec::new()
    )]));
}

#[test]
fn every_item_of_a_cell_batch_is_pointed_at_the_compile_cache() {
    let injected = vec![("SCCACHE_SERVER_PORT".to_string(), "4227".to_string())];
    let batch = with_cell_client_env(
        ProcessBatch {
            files: Vec::new(),
            runtime_packages: Vec::new(),
            sequential: false,
            stop_on_error: true,
            sandbox_mode: ProcessSandboxMode::Unconfined,
            timeout_bases: Vec::new(),
            items: vec![
                cache_batch_item(Vec::new()),
                cache_batch_item(vec![(
                    "SCCACHE_SERVER_PORT".to_string(),
                    "4300".to_string(),
                )]),
            ],
            runner_context_id: None,
            execution_residency: None,
            containment: None,
            node_scratch: None,
        },
        &injected,
    );

    assert_eq!(batch.items[0].env, injected);
    // A caller that named the variable itself meant it.
    assert_eq!(
        batch.items[1].env,
        vec![("SCCACHE_SERVER_PORT".to_string(), "4300".to_string())]
    );
}

/// A cell builds where the daemon's grant reaches; the project's live
/// checkout does not. That difference is not a lost cache hit — the daemon
/// runs each cache-miss compile itself, so a live-checkout build pointed at
/// it fails outright with `Operation not permitted`.
#[tokio::test]
async fn configured_build_service_env_is_limited_to_cells() {
    let temp = tempfile::tempdir().unwrap();
    let orch = test_orchestrator(temp.path()).await;

    let cell = cell_build_service_env(
        &orch,
        &RepositoryLocator::ManagedObjects {
            project_id: "p".into(),
            repository_id: "r".into(),
            object_format: cairn_common::executor_protocol::GitObjectFormat::Sha1,
        },
    );
    assert!(
        cell.is_empty(),
        "without an explicit buildServices setting, cells receive no daemon env: {cell:?}"
    );

    let live = cell_build_service_env(
        &orch,
        &RepositoryLocator::ExistingCheckout {
            project_id: "p".into(),
            repository_id: "r".into(),
            absolute_path: "/home/u/projects/cairn".into(),
        },
    );
    assert!(
        live.is_empty(),
        "a batch in the developer's own checkout must keep its own cache: {live:?}"
    );
}

#[tokio::test]
async fn disconnected_lifetime_lease_is_unavailable_not_not_found() {
    let temp = tempfile::tempdir().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    let result = orch
        .fleet
        .operate_residency(
            &orch,
            ResidencyOperation::RefreshCheckout {
                fence: ResidencyFence {
                    holder: ResidencyHolder::Job {
                        job_id: "retained-on-disconnected-executor".into(),
                    },
                    incarnation_id: "incarnation".into(),
                    cell_epoch: 1,
                },
                base_commit: "new-head".into(),
                require_clean: false,
            },
        )
        .await;

    assert!(matches!(
        result,
        ResidencyResult::Failed {
            kind: ResidencyFailureKind::Unavailable,
            ..
        }
    ));
}

/// A deadline fixed before an operation queues measures the queue. The
/// budget belongs to the operation's own waiting, so it has to start where
/// the queueing ended — otherwise an acquisition that waited out its turn
/// arrives already expired and fails for the waiting rather than for
/// anything about the environment it asked for.
/// The liveness report is what makes a long wait horizon safe, so it has to
/// be honest in both directions: a request whose caller was dropped must fall
/// out of it, and a request with even one live subscriber must stay in.
///
/// Reporting a live request as absent evicts work somebody is waiting for.
/// Reporting a dead one as present reintroduces the phantom queue slot the
/// short deadline used to prevent.
#[test]
fn the_waiting_report_names_exactly_the_requests_with_a_live_waiter() {
    let pool = Fleet::default();
    let (live_tx, _live_rx) = oneshot::channel::<CellOutcome>();
    let (abandoned_tx, abandoned_rx) = oneshot::channel::<CellOutcome>();
    drop(abandoned_rx);
    // The abandoned waiter's receiver is already dropped, which is exactly
    // what "nobody is waiting for this" looks like from the queue's side.
    drop((live_tx, abandoned_tx));
    let live_key = ("live".to_string(), "attempt".to_string());
    let abandoned_key = ("abandoned".to_string(), "attempt".to_string());
    let _live_holder = claim_entry(&pool, &live_key, COLOCATED_EXECUTOR_ID, 1, None);
    drop(claim_entry(
        &pool,
        &abandoned_key,
        COLOCATED_EXECUTOR_ID,
        1,
        None,
    ));
    assert_eq!(
        pool.waiting_request_ids(COLOCATED_EXECUTOR_ID, 1),
        vec!["live".to_string()]
    );

    // A queued acquisition is nameable too, by the entry id both sides derive
    // from the holder rather than by a correlation only the runner knows.
    let holder = ResidencyHolder::Job {
        job_id: "job-1".into(),
    };
    let _acquire_rx = claim_entry(
        &pool,
        &(
            residency_queue_entry_id(&holder),
            RESIDENCY_ACQUIRE_ATTEMPT_ID.into(),
        ),
        COLOCATED_EXECUTOR_ID,
        1,
        None,
    );
    assert_eq!(
        pool.waiting_request_ids(COLOCATED_EXECUTOR_ID, 1),
        vec!["live".to_string(), residency_queue_entry_id(&holder)]
    );

    // An empty report is a legitimate statement, not a missing one: it is how
    // an idle runner frees every slot it was holding.
    drop(_live_holder);
    pool.queue
        .fail_claims_for(COLOCATED_EXECUTOR_ID, "test teardown", unix_time_ms());
    pool.pending_residency.lock().unwrap().clear();
    assert!(pool
        .waiting_request_ids(COLOCATED_EXECUTOR_ID, 1)
        .is_empty());
}

/// A report tells one link about its own waiters and nobody else's.
///
/// Request ids are not globally unique. An acquisition's queue entry id is
/// derived from its holder, so two executors serving the same job mint the
/// identical string — which is exactly the case a report has to get right,
/// because asserting liveness for a waiter that belongs to another link keeps
/// an abandoned entry alive for as long as the other link's waiter lives, and
/// a slot held for nobody is what the liveness window exists to free.
///
/// The generation is part of the scope for the same reason: a waiter recorded
/// against a link that has since bounced says nothing about its replacement.
#[test]
fn a_waiting_report_never_asserts_liveness_for_another_link() {
    let pool = Fleet::default();
    let holder = ResidencyHolder::Job {
        job_id: "shared-job".into(),
    };
    let shared_entry_id = residency_queue_entry_id(&holder);

    let _remote_rx = claim_entry(
        &pool,
        &(shared_entry_id.clone(), "remote".into()),
        "remote-executor",
        1,
        None,
    );
    let _stale_rx = claim_entry(
        &pool,
        &(shared_entry_id.clone(), "stale".into()),
        COLOCATED_EXECUTOR_ID,
        1,
        None,
    );

    assert_eq!(
        pool.waiting_request_ids("remote-executor", 1),
        vec![shared_entry_id.clone()],
        "the link that owns the waiter is told about it"
    );
    assert!(
        pool.waiting_request_ids(COLOCATED_EXECUTOR_ID, 2)
            .is_empty(),
        "a live waiter on another link, and on a bounced generation of this one, must not \
             assert liveness for an entry queued here"
    );
    assert_eq!(
        pool.waiting_request_ids(COLOCATED_EXECUTOR_ID, 1),
        vec![shared_entry_id],
        "the generation that does own a waiter still hears about it"
    );
}

#[tokio::test(start_paused = true)]
async fn residency_responses_are_bounded_by_their_link() {
    let pool = Fleet::default();
    let (sender, _executor) = mpsc::unbounded_channel();
    let generation = pool.attach_executor(sender);
    let (tx, rx) = oneshot::channel::<u8>();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(RESIDENCY_RESPONSE_FLOOR_MS * 6)).await;
        let _ = tx.send(7);
    });
    assert!(matches!(
        pool.await_link_bound_response(rx, COLOCATED_EXECUTOR_ID, generation)
            .await,
        LinkBoundResponse::Answered(7)
    ));
    let (_tx, rx) = oneshot::channel::<u8>();
    assert!(matches!(
        pool.await_link_bound_response(rx, "absent", 1).await,
        LinkBoundResponse::LinkSilent
    ));
    let (tx, rx) = oneshot::channel::<u8>();
    drop(tx);
    assert!(matches!(
        pool.await_link_bound_response(rx, COLOCATED_EXECUTOR_ID, generation)
            .await,
        LinkBoundResponse::Dropped
    ));
}

#[test]
fn acquisition_identity_is_held_by_the_queue_and_released_on_drop() {
    let pool = Fleet::default();
    let holder = ResidencyHolder::Job {
        job_id: "one".into(),
    };
    let request = residency_placement_request(&fleet_residency_request(holder));
    let identity = (request.request_id.clone(), request.attempt_id.clone());
    let new_entry = || queue::NewEntry {
        request: request.clone(),
        admission_kind: CellAdmissionKind::Residency,
        interactive: true,
        execution_residency: None,
        standing: false,
    };
    let (entry_id, _rx) = pool.queue.enqueue(new_entry(), unix_time_ms()).unwrap();
    assert!(pool.queue.enqueue(new_entry(), unix_time_ms()).is_err());
    let guard = EntryDropGuard {
        pool: pool.clone(),
        request_id: identity.0.clone(),
        attempt_id: identity.1.clone(),
        entry_id,
        granted_to: None,
        cause: None,
        armed: true,
    };
    drop(guard);
    assert!(pool.queue.enqueue(new_entry(), unix_time_ms()).is_ok());
}

/// One job's acquisition must never gate another's. Placement waits for as
/// long as the chosen executor keeps reporting progress, so a gate spanning
/// the fleet turns one job's cold start into every other job's stall — and
/// the stalled jobs then fail on deadlines the stall itself consumed.
#[tokio::test]
async fn acquiring_one_environment_never_gates_another() {
    let temp = tempfile::tempdir().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    let wedged = ResidencyHolder::Job {
        job_id: "wedged".into(),
    };
    let request = residency_placement_request(&fleet_residency_request(wedged));
    let (_, held) = orch
        .fleet
        .queue
        .enqueue(
            queue::NewEntry {
                request,
                admission_kind: CellAdmissionKind::Residency,
                interactive: true,
                execution_residency: None,
                standing: false,
            },
            unix_time_ms(),
        )
        .unwrap();

    let unrelated = fleet_residency_request(ResidencyHolder::Job {
        job_id: "unrelated".into(),
    });
    let answered = tokio::time::timeout(
        Duration::from_secs(5),
        orch.fleet
            .operate_residency(&orch, ResidencyOperation::Acquire { request: unrelated }),
    )
    .await
    .expect("an unrelated environment's acquisition must not wait behind a wedged one");
    assert!(matches!(answered, ResidencyResult::Failed { .. }));
    drop(held);
}

/// A request whose horizon elapsed while a live substrate kept reporting
/// progress has to say so. Untargeted requests reach here — a job's
/// execution environment names no executor — so a diagnostic that assumed
/// a selector panicked at exactly the point the wait had already run out.
#[tokio::test]
async fn an_elapsed_horizon_under_a_live_substrate_diagnoses_rather_than_panicking() {
    let pool = Fleet::default();
    pool.declare_colocated_substrate(ExecutorSubstrateState::CapacityBusy);
    let mut request = targeted_request("linux");
    request.executor = None;
    request.wait_horizon_unix_ms = unix_time_ms().saturating_sub(1);

    let outcome = place_through_dispatcher(&pool, &request, None)
        .await
        .unwrap_err();

    let CellOutcome::Unavailable { reason, diagnostic } = outcome else {
        panic!("an elapsed horizon leaves no executor to run on");
    };
    assert!(matches!(
        reason,
        CellUnavailableReason::NoMatchingExecutor { .. }
    ));
    assert!(
        diagnostic.contains("before this request's wait horizon"),
        "{diagnostic}"
    );
}

fn result_identity() -> CheckResultIdentity {
    CheckResultIdentity::new("project", "check", "input")
}

/// A machine whose page-call probe did not run a program a page-call batch
/// needs is passed over, and says which program and why.
#[test]
fn a_page_call_program_the_probe_did_not_run_is_refused_by_name() {
    use cairn_common::executor_protocol::PageCallProgramEvidence;
    let mut capabilities = capabilities_with_cores(8);
    let selector = ExecutorSelector {
        page_call_programs: vec!["python3".into()],
        ..ExecutorSelector::default()
    };
    assert_eq!(
        capabilities
            .page_call_program_shortfall("python3")
            .as_deref(),
        Some("page-call policy has not been measured running python3")
    );
    capabilities.page_call_programs = Some(vec![PageCallProgramEvidence {
        program: "python3".into(),
        passed: false,
        resolved: Some("/usr/bin/python3".into()),
        detail: "exited 1 under the page-call policy: Permission denied".into(),
    }]);
    assert_eq!(
        capabilities.page_call_program_shortfall("python3").as_deref(),
        Some("page-call policy cannot run python3: exited 1 under the page-call policy: Permission denied")
    );
    capabilities.page_call_programs.as_mut().unwrap()[0].passed = true;
    assert_eq!(capabilities.page_call_program_shortfall("python3"), None);
    assert_eq!(
        capabilities.page_call_program_shortfall("bun").as_deref(),
        Some("page-call policy has not been measured running bun")
    );
    assert!(!selector.is_empty());
    assert!(selector.describe().contains("page-call programs python3"));
}

fn capabilities_with_cores(logical_cores: usize) -> ExecutorCapabilities {
    ExecutorCapabilities {
        os: "macos".into(),
        arch: "aarch64".into(),
        logical_cores,
        concurrency_capacity: None,
        toolchains: Vec::new(),
        projects_served: Vec::new(),
        disk_budget_bytes: None,
        memory_budget_bytes: None,
        sandbox: None,
        toolchain_detection: None,
        build_prerequisites: None,
        containment: None,
        page_call_programs: None,
        github_credential: None,
        publish_isolation: None,
        machine_services: Vec::new(),
        credential_floor_service_residents: false,
        resident_output_end: false,
        service_bounds: None,
    }
}

#[test]
fn measured_nested_sandbox_capability_controls_only_sensitive_placement() {
    let selector = ExecutorSelector {
        requires_macos_nested_sandbox_write: true,
        ..ExecutorSelector::default()
    };
    let mut capabilities = capabilities_with_cores(8);

    assert!(
        !matches_sandbox_capability(&capabilities, &selector),
        "an old or incapable macOS executor must not receive sandbox-sensitive work"
    );
    assert!(
        matches_sandbox_capability(&capabilities, &ExecutorSelector::default()),
        "ordinary work remains placeable"
    );

    // An executor that measured from its own process, as every executor
    // did before cairn/5316: its `available` is the claim a machine whose
    // lanes all exited 71 kept making, so it places nothing.
    let mut evidence = SandboxCapabilityEvidence {
        cairn_sandboxed_marker: false,
        nested_sandbox_write_available: true,
        nested_sandbox_write_detail: "live probe succeeded".into(),
        nested_sandbox_write_context: None,
        violation_recovery: None,
    };
    capabilities.sandbox = Some(evidence.clone());
    assert!(!matches_sandbox_capability(&capabilities, &selector));
    assert!(nested_sandbox_summary(&evidence).contains("unverified"));

    let context = cairn_common::executor_protocol::NestedSandboxProbeContext {
        launcher: cairn_common::executor_protocol::SandboxLauncherClass::Unconfined,
        credential_floor_sha256: None,
        outer_profile_sha256: None,
        fingerprint: Some("f".into()),
        measured_at_unix_ms: 1,
    };
    evidence.nested_sandbox_write_context = Some(context.clone());
    capabilities.sandbox = Some(evidence.clone());
    assert!(matches_sandbox_capability(&capabilities, &selector));

    // A check lane under the credential floor: measured, refused, and the
    // inventory a refusal prints carries the kernel rule as the reason.
    let floored = SandboxCapabilityEvidence {
        cairn_sandboxed_marker: false,
        nested_sandbox_write_available: false,
        nested_sandbox_write_detail:
            "outer profile inherited: a confined process cannot apply a second profile".into(),
        nested_sandbox_write_context: Some(
            cairn_common::executor_protocol::NestedSandboxProbeContext {
                credential_floor_sha256: Some("floor".into()),
                outer_profile_sha256: Some("outer".into()),
                ..context
            },
        ),
        violation_recovery: None,
    };
    capabilities.sandbox = Some(floored.clone());
    assert!(!matches_sandbox_capability(&capabilities, &selector));
    let summary = nested_sandbox_summary(&floored);
    assert!(
        summary.contains("check-lane nested Seatbelt write=unavailable (outer profile inherited: a confined process cannot apply a second profile)"),
        "{summary}"
    );
    assert!(
        matches_sandbox_capability(&capabilities, &ExecutorSelector::default()),
        "an incapable machine still takes work that does not need the capability"
    );
}

/// Whole-machine demand is declared as saturation, because a submitter cannot
/// know which executor will take the work. Resolving it must yield the
/// executor's ENTIRE admission budget: anything less leaves headroom, and a
/// lane that declared the whole machine would run beside something else.
///
/// Asserted against the budget function the executor itself admits against,
/// so the two cannot drift back apart. A single-core host is the case that
/// caught the drift — its budget floor is 2, not 1.
#[test]
fn whole_machine_demand_resolves_to_the_entire_budget_of_its_executor() {
    let whole_machine = ResourceReservation::WHOLE_MACHINE_CONCURRENCY;
    for logical_cores in [0, 1, 2, 8, 12] {
        let capabilities = capabilities_with_cores(logical_cores);
        let resolved = clamp_declared_concurrency(whole_machine, &capabilities);
        assert_eq!(
            resolved,
            capabilities.admission_concurrency_budget(),
            "a whole-machine lane must consume the complete budget of a \
                 {logical_cores}-core executor, leaving no room beside it"
        );
        assert!(
            resolved >= 2,
            "the budget floor is two, so a one-core host cannot resolve to one"
        );
    }
}

#[test]
fn colocated_whole_machine_demand_uses_advertised_effective_capacity() {
    let mut capabilities = capabilities_with_cores(16);
    capabilities.concurrency_capacity = Some(cairn_common::executor_protocol::executor_cpu_budget(
        16, true,
    ));
    assert_eq!(
        clamp_declared_concurrency(
            ResourceReservation::WHOLE_MACHINE_CONCURRENCY,
            &capabilities,
        ),
        14
    );
}

fn containment_capabilities(
    containment: Option<cairn_common::executor_protocol::ContainmentObservations>,
) -> ExecutorCapabilities {
    ExecutorCapabilities {
        os: "macos".into(),
        arch: "aarch64".into(),
        logical_cores: 8,
        concurrency_capacity: None,
        toolchains: Vec::new(),
        projects_served: Vec::new(),
        disk_budget_bytes: None,
        memory_budget_bytes: None,
        sandbox: None,
        toolchain_detection: None,
        build_prerequisites: None,
        containment,
        page_call_programs: None,
        github_credential: None,
        publish_isolation: None,
        machine_services: Vec::new(),
        credential_floor_service_residents: false,
        resident_output_end: false,
        service_bounds: None,
    }
}

fn batch_envelope(
    edge: cairn_common::executor_protocol::EdgeId,
) -> cairn_common::executor_protocol::WorkloadEnvelope {
    use cairn_common::executor_protocol::{EdgeRequirement, MaximumDisposition, WorkloadEnvelope};
    WorkloadEnvelope {
        process_class: edge.definition().key.process_class,
        edges: vec![EdgeRequirement {
            edge,
            maximum_disposition: MaximumDisposition::Refused,
        }],
    }
}

/// The check adapter must not sever the envelope between admission and
/// launch.
///
/// Checks are a primary batch producer, so an adapter that evaluates a
/// containment selector at placement and then hands the executor a batch
/// with no envelope would leave the admission-to-spawn window open for
/// exactly the workloads the cadence runs most often. The runner would have
/// enforced the requirement and the executor would have nothing to re-check.
#[test]
fn a_check_batch_carries_the_envelope_it_was_placed_on() {
    use cairn_common::executor_protocol::EdgeId;

    let envelope = batch_envelope(EdgeId::BatchSpawnFilesystemConfined);
    let mut request = provenance_request();
    request.executor = Some(ExecutorSelector {
        containment: Some(envelope.clone()),
        ..ExecutorSelector::default()
    });

    assert_eq!(
        Fleet::check_batch_containment(&request),
        Some(envelope.clone()),
        "the envelope placement matched on is the one the batch carries"
    );
    let batch = Fleet::check_process_batch(
        Vec::new(),
        Some("ctx".into()),
        Fleet::check_batch_containment(&request),
        Fleet::WRITE_CHECK_SILENCE_BOUND_MS,
    );
    assert_eq!(
        batch.containment,
        Some(envelope),
        "the check adapter must not drop a non-empty envelope"
    );
}

/// And a check that asked for nothing is unchanged: the fence is opt-in, so
/// adding the field cannot start refusing work nobody constrained.
#[test]
fn a_check_batch_without_an_envelope_is_unchanged() {
    let mut request = provenance_request();
    request.executor = Some(ExecutorSelector {
        os: Some("macos".into()),
        ..ExecutorSelector::default()
    });
    assert_eq!(Fleet::check_batch_containment(&request), None);

    let batch = Fleet::check_process_batch(
        Vec::new(),
        Some("ctx".into()),
        Fleet::check_batch_containment(&request),
        Fleet::WRITE_CHECK_SILENCE_BOUND_MS,
    );
    assert_eq!(batch.containment, None);

    // No selector at all is the ordinary case and stays the ordinary case.
    let bare = provenance_request();
    assert_eq!(Fleet::check_batch_containment(&bare), None);
}

/// An executor that reports nothing is refused, and is told apart from one
/// that measured an edge and found it wanting. Both refuse; they send an
/// operator to different places, so the words have to differ too.
#[test]
fn an_executor_without_containment_reporting_is_refused_in_its_own_terms() {
    use cairn_common::executor_protocol::EdgeId;
    let selector = ExecutorSelector {
        containment: Some(batch_envelope(EdgeId::BatchSpawnFilesystemConfined)),
        ..ExecutorSelector::default()
    };
    let reason = containment_deficiency(&containment_capabilities(None), &selector)
        .expect("an unreporting executor cannot satisfy an envelope");
    assert!(
        reason.contains("predates containment reporting"),
        "{reason}"
    );
}

/// A parallel batch runs as many items at once as it was charged for, so it
/// is charged for every item it runs together. Charging only its heaviest
/// item, as a sequential batch rightly is, ran an agent's parallel items one
/// after another.
#[tokio::test]
async fn a_parallel_batch_is_charged_for_every_item_it_runs_at_once() {
    let db = Arc::new(crate::storage::migrated_test_db("parallel-batch-width.db").await);
    let mut request = provenance_request();
    request.command_class = CellCommandClass::Other;
    let batch = |sequential: bool, items: usize| ProcessBatch {
        files: Vec::new(),
        runtime_packages: Vec::new(),
        sequential,
        stop_on_error: false,
        sandbox_mode: ProcessSandboxMode::Unconfined,
        timeout_bases: Vec::new(),
        items: (0..items).map(|_| cache_batch_item(Vec::new())).collect(),
        runner_context_id: None,
        execution_residency: None,
        containment: None,
        node_scratch: None,
    };
    let units = |batch: ProcessBatch, capabilities: ExecutorCapabilities| {
        let (db, request) = (db.clone(), request.clone());
        async move {
            let resolved = ReservationPlan::new(db, &request, Some(&batch))
                .resolve_for(
                    &request,
                    "device",
                    "executor",
                    &capabilities,
                    0,
                    resource_profiles::DurationContext {
                        class: request.command_class,
                        warmth: ExecutionWarmth::Cold,
                        now_unix_ms: NOW,
                    },
                )
                .await;
            assert!(
                resolved.reservation.memory_charge_bytes() > 0,
                "members without identities retain their conservative class prior"
            );
            resolved.reservation.concurrency_units
        }
    };

    assert_eq!(units(batch(false, 3), capabilities_with_cores(16)).await, 3);
    assert_eq!(
        units(batch(true, 3), capabilities_with_cores(16)).await,
        1,
        "a sequential batch runs one item at a time"
    );
    let small = capabilities_with_cores(2);
    assert_eq!(
        units(batch(false, 40), small.clone()).await,
        small.admission_concurrency_budget(),
        "a batch wider than the machine runs in waves rather than never fitting"
    );

    // A compile takes one compile slot, whose width its items split, sequential
    // or parallel (cairn/7303): 16 cores are two slots of eight.
    let mut compile = provenance_request();
    compile.command_class = CellCommandClass::CargoCheck;
    for sequential in [true, false] {
        let resolved = ReservationPlan::new(db.clone(), &compile, Some(&batch(sequential, 3)))
            .resolve_for(
                &compile,
                "device",
                "executor",
                &capabilities_with_cores(16),
                0,
                resource_profiles::DurationContext {
                    class: compile.command_class,
                    warmth: ExecutionWarmth::Cold,
                    now_unix_ms: NOW,
                },
            )
            .await;
        assert_eq!(resolved.reservation.concurrency_units, 8, "{sequential}");
    }

    let capabilities = capabilities_with_cores(12);
    let duration = resource_profiles::DurationContext {
        class: compile.command_class,
        warmth: ExecutionWarmth::Cold,
        now_unix_ms: NOW,
    };
    // Both a single compile and either batch shape follow the current net
    // slot width; a standing change must replace, not reuse, the memoized need.
    for batch in [None, Some(batch(true, 3)), Some(batch(false, 3))] {
        let plan = ReservationPlan::new(db.clone(), &compile, batch.as_ref());
        for (standing, expected, worked_out) in
            [(0, 12, 1), (2, 10, 1), (2, 10, 0), (3, 9, 1), (0, 12, 1)]
        {
            let resolved = plan
                .resolve_for(
                    &compile,
                    "device",
                    "executor",
                    &capabilities,
                    standing,
                    duration,
                )
                .await;
            assert_eq!(resolved.reservation.concurrency_units, expected);
            assert_eq!(plan.take_needs_worked_out(), worked_out);
            assert_eq!(plan.needs_held(), 1);
        }
    }
}

/// A minted identity is no licence to its class suite's build-job figures,
/// which some other command of the class measured. A 30 GiB largest job
/// learned under `class:cargoTest` here must not make `cargo test -p
/// cairn-codec` too large for a 20 GiB executor bound (the test placement's
/// `executor_bound_rejection` applies); the same figures learned under a
/// configured suite still do.
#[tokio::test]
async fn a_class_suites_build_jobs_never_price_an_ad_hoc_compile() {
    const GIB: u64 = 1 << 30;
    let db = Arc::new(crate::storage::migrated_test_db("class-suite-build-jobs.db").await);
    let mut capabilities = capabilities_with_cores(16);
    capabilities.memory_budget_bytes = Some(120 * GIB);
    let context = ReservationPlan::context_for("device", "executor", &capabilities);
    let request = provenance_request();
    let census = CellExecutionMeta {
        stop: None,
        peak_process: Some(cairn_common::executor_protocol::PeakProcess {
            bytes: 30 * GIB,
            name: Some("rustc".into()),
        }),
        build_job_processes: Some(cairn_common::executor_protocol::BuildJobProcesses {
            processes: 40,
            largest_bytes: 30 * GIB,
            typical_bytes: GIB,
        }),
        ..resource_profiles::oom_killed_for_test(GIB, None, 0, 1)
    };
    for suite in ["class:cargoTest", "rust-tests"] {
        resource_profiles::observe_suite(
            db.clone(),
            Some(&format!("{}/{suite}", request.project_id)),
            CellCommandClass::CargoTest,
            &context,
            &census,
            resource_profiles::CompletionKind::Succeeded,
            resource_profiles::MachineAdmission::default(),
            resource_profiles::CompletedSource::default(),
        )
        .await;
    }
    let resolve = |item: ProcessBatchItem| {
        let (db, request, capabilities) = (db.clone(), request.clone(), capabilities.clone());
        async move {
            let batch = ProcessBatch {
                files: Vec::new(),
                runtime_packages: Vec::new(),
                sequential: true,
                stop_on_error: false,
                sandbox_mode: ProcessSandboxMode::Unconfined,
                timeout_bases: Vec::new(),
                items: vec![item],
                runner_context_id: None,
                execution_residency: None,
                containment: None,
                node_scratch: None,
            };
            ReservationPlan::new(db, &request, Some(&batch))
                .resolve_for(
                    &request,
                    "device",
                    "executor",
                    &capabilities,
                    0,
                    resource_profiles::DurationContext {
                        class: CellCommandClass::CargoTest,
                        warmth: ExecutionWarmth::Cold,
                        now_unix_ms: NOW,
                    },
                )
                .await
        }
    };
    let ad_hoc = ProcessBatchItem {
        args: vec!["cargo test -p cairn-codec --lib".into()],
        ..cache_batch_item(Vec::new())
    };
    let configured = ProcessBatchItem {
        command_resource_identity: Some(CommandResourceIdentity {
            version: 1,
            key: "rust-tests:cold".into(),
            suite: Some("rust-tests".into()),
        }),
        ..ad_hoc.clone()
    };
    let bound = 20 * GIB;

    let small = resolve(ad_hoc).await;
    assert_eq!(small.rationale.slot_price, None);
    assert!(
        small.reservation.memory_charge_bytes() <= bound,
        "another command's 30 GiB job never refuses this one at the bound: {:?}",
        small.reservation
    );
    // Measured once on another machine, the command is charged its own
    // largest process here, never the 2 GiB prior again.
    let identity = resource_profiles::ad_hoc_identity(
        &request.project_id,
        "cargo test -p cairn-codec --lib",
        CellCommandClass::CargoTest,
    )
    .unwrap();
    resource_profiles::observe_completed(
        db.clone(),
        Some(&identity),
        &resource_profiles::ProfileContext {
            executor_class: "elsewhere-device:elsewhere".into(),
            ..context.clone()
        },
        &CellExecutionMeta {
            stop: None,
            peak_process: Some(cairn_common::executor_protocol::PeakProcess {
                bytes: 3 * GIB,
                name: Some("rustc".into()),
            }),
            ..resource_profiles::oom_killed_for_test(3 * GIB, None, 0, 2)
        },
        resource_profiles::CompletionKind::Succeeded,
        resource_profiles::CompletedSource::default(),
    )
    .await;
    let measured = resolve(ProcessBatchItem {
        args: vec!["cargo test -p cairn-codec --lib -- one::test".into()],
        ..cache_batch_item(Vec::new())
    })
    .await;
    assert!(
        measured.reservation.memory_charge_bytes() >= 3 * GIB / 4 * 5,
        "a command measured on another machine is charged its own process: {:?}",
        measured.reservation
    );
    let suite = resolve(configured).await;
    assert!(
        suite.reservation.memory_charge_bytes() > bound,
        "a configured suite's measured 30 GiB process is still refused at a 20 GiB bound: {:?}",
        suite.reservation
    );
}

/// A reservation has two independent halves, and the record must carry both.
///
/// Concurrency comes from the caller and memory/disk come from the learned
/// profile, so an explanation that names only one of them describes a number
/// nobody chose. This is the record that made a caller-declared whole-machine
/// charge read as "learned from 1 observation, fell back because
/// belowConfidenceFloor" — an explanation for the memory estimate, attached
/// to a concurrency figure that learning cannot produce (CAIRN-3345).
#[tokio::test]
async fn a_declared_concurrency_and_a_learned_lookup_are_both_on_the_record() {
    let db = Arc::new(crate::storage::migrated_test_db("reservation-provenance.db").await);
    let capabilities = capabilities_with_cores(16);
    let mut request = provenance_request();
    // Not a compile, which asks at least one compile slot whatever it
    // declares: this is about whose number the declared half is.
    request.command_class = CellCommandClass::Other;
    request.resource_reservation = ResourceReservation {
        memory_bytes: 0,
        disk_growth_bytes: 0,
        concurrency_units: 1,
        source: ResourceReservationSource::Declared,
        measured_peak_bytes: None,
        test_phase: None,
    };
    let resolved = ReservationPlan::new(db.clone(), &request, None)
        .resolve_for(
            &request,
            "device",
            "executor",
            &capabilities,
            0,
            resource_profiles::DurationContext {
                class: request.command_class,
                warmth: ExecutionWarmth::Cold,
                now_unix_ms: NOW,
            },
        )
        .await;
    assert_eq!(
        resolved.rationale.declared_concurrency_units,
        Some(1),
        "the declared half says who declared it"
    );
    assert_eq!(resolved.reservation.concurrency_units, 1);
    assert_eq!(
        resolved.reservation.source,
        ResourceReservationSource::Unmeasured,
        "declaring CPU concurrency must not declare memory or disable width pricing"
    );
    assert_eq!(
        resolved.rationale.fallback,
        Some(cairn_common::executor_protocol::ReservationFallback::NoProfileRecorded),
        "and the learned half still explains itself in its own terms"
    );

    // An exclusive check declares saturation, and that too is declared
    // provenance rather than a conclusion drawn from a thin profile.
    request.resource_reservation.concurrency_units = ResourceReservation::WHOLE_MACHINE_CONCURRENCY;
    let exclusive = ReservationPlan::new(db.clone(), &request, None)
        .resolve_for(
            &request,
            "device",
            "executor",
            &capabilities,
            0,
            resource_profiles::DurationContext {
                class: request.command_class,
                warmth: ExecutionWarmth::Cold,
                now_unix_ms: NOW,
            },
        )
        .await;
    assert_eq!(
        exclusive.reservation.concurrency_units,
        capabilities.admission_concurrency_budget()
    );
    assert_eq!(
        exclusive.rationale.declared_concurrency_units,
        Some(capabilities.admission_concurrency_budget())
    );
    request.resource_reservation.concurrency_units = 0;
    let zero = ReservationPlan::new(db.clone(), &request, None)
        .resolve_for(
            &request,
            "device",
            "executor",
            &capabilities,
            0,
            resource_profiles::DurationContext {
                class: request.command_class,
                warmth: ExecutionWarmth::Cold,
                now_unix_ms: NOW,
            },
        )
        .await;
    assert_ne!(zero.reservation.source, ResourceReservationSource::Declared);
    assert_eq!(zero.rationale.declared_concurrency_units, Some(0));
    request.resource_reservation.memory_bytes = 123_456;
    let stated = ReservationPlan::new(db, &request, None)
        .resolve_for(
            &request,
            "device",
            "executor",
            &capabilities,
            0,
            resource_profiles::DurationContext {
                class: request.command_class,
                warmth: ExecutionWarmth::Cold,
                now_unix_ms: NOW,
            },
        )
        .await;
    assert_eq!(stated.reservation.memory_bytes, 123_456);
    assert_eq!(
        stated.reservation.source,
        ResourceReservationSource::Declared
    );
}

/// Put an entry in the queue in exactly the state a real dispatch leaves it:
/// offered to this machine, then claimed by it.
///
/// These tests used to build that state by inserting into a map of in-flight
/// requests. Driving the queue's own transitions instead is what makes them
/// tests of the state machine dispatch actually uses, rather than of a shape
/// that merely resembled it.
fn claim_entry(
    pool: &Fleet,
    key: &RequestIdentity,
    executor_id: &str,
    generation: u64,
    requesting_job_id: Option<&str>,
) -> oneshot::Receiver<CellOutcome> {
    let (entry_id, receiver) = enqueue_entry(pool, key, requesting_job_id);
    assert!(
        pool.queue.offer(
            &entry_id,
            executor_id,
            executor_id,
            generation,
            unix_time_ms()
        ),
        "a freshly queued entry can be offered"
    );
    assert_eq!(
        pool.queue
            .claim(&entry_id, executor_id, generation, unix_time_ms()),
        queue::ClaimVerdict::Granted,
        "the machine the entry was offered to can claim it"
    );
    receiver
}

fn enqueue_entry(
    pool: &Fleet,
    key: &RequestIdentity,
    requesting_job_id: Option<&str>,
) -> (String, oneshot::Receiver<CellOutcome>) {
    let mut request = provenance_request();
    request.request_id.clone_from(&key.0);
    request.attempt_id.clone_from(&key.1);
    request.requesting_job_id = requesting_job_id.map(str::to_string);
    request.wait_horizon_unix_ms = unix_time_ms() + 60_000;
    pool.queue
        .enqueue(
            queue::NewEntry {
                request,
                admission_kind: CellAdmissionKind::Command,
                interactive: false,
                execution_residency: None,
                standing: false,
            },
            unix_time_ms(),
        )
        .expect("a fresh request identity enqueues")
}

/// A pure-verdict execution led by `leader`, with its own check and one
/// other request's check coalesced onto it, under the identities
/// [`Fleet::check_subscriber`] mints.
fn dispatched_check(pool: &Fleet, leader: &RequestIdentity) -> (RequestIdentity, RequestIdentity) {
    let own = (format!("{}:check", leader.0), format!("{}:check", leader.1));
    let joined = (
        "other-request:check".to_string(),
        "other-attempt:check".to_string(),
    );
    let subscriber = || {
        let (waiter, _) = oneshot::channel();
        CoalescedSubscriber {
            waiter,
            priority: CellPriority::ReviewCheck,
            requesting_job_id: None,
        }
    };
    pool.in_flight.lock().unwrap().open(
        CoalescingKey::new(result_identity(), leader.clone()),
        PlacementDemand::default(),
        HashMap::from([(own.clone(), subscriber()), (joined.clone(), subscriber())]),
    );
    (own, joined)
}

fn enqueue_owned_entry(pool: &Fleet, key: &RequestIdentity) -> String {
    let mut request = provenance_request();
    request.request_id.clone_from(&key.0);
    request.attempt_id.clone_from(&key.1);
    request.owner = Some(cairn_common::executor_protocol::CellOwnerRef {
        project_id: "p".into(),
        project_key: Some("cairn".into()),
        issue_number: Some(5178),
        job_id: None,
        execution_seq: None,
        node_kind: Some("builder".into()),
    });
    request.waiting_since_unix_ms = 1_000;
    pool.queue
        .enqueue(
            queue::NewEntry {
                request,
                admission_kind: CellAdmissionKind::Command,
                interactive: false,
                execution_residency: None,
                standing: false,
            },
            unix_time_ms(),
        )
        .expect("a fresh request identity enqueues")
        .0
}

/// A run's wait text is the queued request's account, the one the check row
/// and the placement view read, until a machine claims it.
#[test]
fn unplaced_wait_quotes_the_queued_account_until_claimed() {
    let pool = Fleet::default();
    let key = ("parked-summary".to_string(), "attempt".to_string());
    let entry = enqueue_owned_entry(&pool, &key);
    let summary = pool.unplaced_wait(&key.0).unwrap();
    assert!(
        summary.starts_with("waiting to be placed since "),
        "{summary}"
    );
    assert!(
        summary.contains("no machine has refused it yet"),
        "{summary}"
    );
    assert!(pool.queue.offer(&entry, "fedora", "fedora", 3, 2_000));
    pool.queue.decline(
        &entry,
        "fedora",
        3,
        DeclineReason::Pressure,
        Some("cpu 97%".into()),
        None,
        2_500,
    );
    let summary = pool.unplaced_wait(&key.0).unwrap();
    assert!(
        summary.contains("declined by fedora (host pressure: cpu 97%)"),
        "{summary}"
    );
    assert!(pool.queue.offer(&entry, "fedora", "fedora", 3, 3_000));
    assert_eq!(
        pool.queue.claim(&entry, "fedora", 3, 3_100),
        queue::ClaimVerdict::Granted
    );
    assert!(pool.unplaced_wait(&key.0).is_none());
}

#[test]
fn check_dispatch_phase_reports_preparation_before_the_queue() {
    let pool = Fleet::default();
    let leader = ("leader".to_string(), "leader-attempt".to_string());
    let (own, _) = dispatched_check(&pool, &leader);
    pool.preparing_leaders.lock().unwrap().insert(
        leader.clone(),
        LeaderPreparation {
            since_unix_ms: 42,
            last_progress_unix_ms: 42,
        },
    );
    assert_eq!(
        pool.check_dispatch_phase(&own),
        Some(CheckDispatch {
            phase: CheckDispatchPhase::Preparing { since_unix_ms: 42 },
            joined: None,
        })
    );
}

#[test]
fn check_dispatch_phase_reports_placing_with_the_latest_decline() {
    let pool = Fleet::default();
    let leader = ("leader".to_string(), "leader-attempt".to_string());
    let (own, _) = dispatched_check(&pool, &leader);
    let entry = enqueue_owned_entry(&pool, &leader);
    assert!(pool.queue.offer(&entry, "fedora", "fedora", 3, 2_000));
    pool.queue.decline(
        &entry,
        "fedora",
        3,
        DeclineReason::Pressure,
        Some("memory floor".into()),
        None,
        2_500,
    );
    assert_eq!(
        pool.check_dispatch_phase(&own),
        Some(CheckDispatch {
            phase: CheckDispatchPhase::Placing {
                since_unix_ms: 1_000,
                turn: None,
                placement: None,
                declines: vec![CheckDispatchDecline {
                    executor_name: "fedora".into(),
                    reason: DeclineReason::Pressure,
                    detail: Some("memory floor".into()),
                    at_unix_ms: 2_500,
                    room_seq: None,
                }],
            },
            joined: None,
        })
    );
}

#[test]
fn check_dispatch_phase_reports_running_from_the_claim_and_who_a_joiner_joined() {
    let pool = Fleet::default();
    let leader = ("leader".to_string(), "leader-attempt".to_string());
    let (own, joined) = dispatched_check(&pool, &leader);
    let entry = enqueue_owned_entry(&pool, &leader);
    assert!(pool.queue.offer(&entry, "fedora", "fedora", 3, 2_000));
    assert_eq!(
        pool.queue.claim(&entry, "fedora", 3, 2_100),
        queue::ClaimVerdict::Granted
    );
    let running = CheckDispatchPhase::Running {
        executor_name: "fedora".into(),
        slot: None,
        since_unix_ms: 2_100,
    };
    let (_, mut connection) = fleet_entry("fedora", "linux", 0, &[]);
    connection.generation = 3;
    let mut execution = executing_fixture(&leader.0);
    execution.attempt_id = "another-attempt".into();
    execution.cell_id = "slot-7".into();
    Arc::make_mut(&mut connection.snapshot).executing_requests = vec![execution];
    pool.lock_connections().insert("fedora".into(), connection);
    assert_eq!(
        pool.check_dispatch_phase(&own),
        Some(CheckDispatch {
            phase: running.clone(),
            joined: None,
        })
    );
    assert_eq!(
        pool.check_dispatch_phase(&joined),
        Some(CheckDispatch {
            phase: running,
            joined: Some("cairn-5178".into()),
        }),
        "a subscriber that coalesced onto another request names whose run it joined"
    );
    {
        let mut connections = pool.lock_connections();
        let connection = connections.get_mut("fedora").unwrap();
        Arc::make_mut(&mut connection.snapshot).executing_requests[0].attempt_id = leader.1.clone();
    }
    assert!(matches!(pool.check_dispatch_phase(&joined).unwrap().phase,
        CheckDispatchPhase::Running { slot: Some(slot), .. } if slot == "slot-7"));
    pool.lock_connections()
        .get_mut("fedora")
        .unwrap()
        .generation = 4;
    assert!(matches!(
        pool.check_dispatch_phase(&own).unwrap().phase,
        CheckDispatchPhase::Running { slot: None, .. }
    ));
}

/// cairn/6499's specimen: a check lane pinned to one machine that refuses it
/// every time. No placement decision names that machine, so its page had
/// nothing to say and the requester read the lane as lost. The queue's own
/// account answers by request id: the machine's refusal verbatim, under the
/// room it came under, and gone once a machine claims it.
#[test]
fn a_queued_request_is_accounted_for_by_request_id_with_the_room_it_was_refused_under() {
    let pool = Fleet::default();
    let key = ("78a872da".to_string(), "attempt".to_string());
    let entry = enqueue_owned_entry(&pool, &key);
    let refusal = "memory short: owed 6.5 GiB + 2.0 GiB for this request > available 6.4 GiB";
    assert!(pool.queue.offer(&entry, "bgsrv-ub", "bgsrv-ub", 3, 2_000));
    pool.queue.decline(
        &entry,
        "bgsrv-ub",
        3,
        DeclineReason::Pressure,
        Some(refusal.into()),
        None,
        2_500,
    );
    pool.queue.note_decline_room(&entry, "bgsrv-ub", 41);
    let Some(CheckDispatchPhase::Placing { declines, .. }) =
        pool.queued_request_account("78a872da")
    else {
        panic!("a queued request is accounted for");
    };
    assert_eq!(declines.len(), 1);
    assert_eq!(declines[0].executor_name, "bgsrv-ub");
    assert_eq!(declines[0].detail.as_deref(), Some(refusal));
    assert_eq!(declines[0].room_seq, Some(41));
    assert_eq!(pool.queued_request_account("unknown"), None);
    assert!(pool.queue.offer(&entry, "fedora", "fedora", 4, 3_000));
    assert_eq!(
        pool.queue.claim(&entry, "fedora", 4, 3_100),
        queue::ClaimVerdict::Granted
    );
    assert_eq!(
        pool.queued_request_account("78a872da"),
        None,
        "a claimed request is no longer queued"
    );
}

/// A write-check batch is queued under its own request identity and never
/// coalesced, so its phase is read by that identity: placing with each
/// machine's decline while held, then running on the machine that claimed
/// it.
#[test]
fn request_dispatch_phase_reads_a_write_check_batch_by_its_own_identity() {
    let pool = Fleet::default();
    let key = ("write-batch".to_string(), "write-attempt".to_string());
    let entry = enqueue_owned_entry(&pool, &key);
    assert!(pool.queue.offer(&entry, "local", "local", 3, 2_000));
    pool.queue.decline(
        &entry,
        "local",
        3,
        DeclineReason::Pressure,
        Some("cpu 97%".into()),
        None,
        2_500,
    );
    assert_eq!(
        pool.request_dispatch_phase(&key),
        Some(CheckDispatchPhase::Placing {
            since_unix_ms: 1_000,
            turn: None,
            placement: None,
            declines: vec![CheckDispatchDecline {
                executor_name: "local".into(),
                reason: DeclineReason::Pressure,
                detail: Some("cpu 97%".into()),
                at_unix_ms: 2_500,
                room_seq: None,
            }],
        })
    );
    assert!(pool.queue.offer(&entry, "fedora", "fedora", 4, 3_000));
    assert_eq!(
        pool.queue.claim(&entry, "fedora", 4, 3_100),
        queue::ClaimVerdict::Granted
    );
    assert_eq!(
        pool.request_dispatch_phase(&key),
        Some(CheckDispatchPhase::Running {
            executor_name: "fedora".into(),
            slot: None,
            since_unix_ms: 3_100,
        })
    );
    assert_eq!(
        pool.request_dispatch_phase(&("unknown".into(), "unknown".into())),
        None
    );
}

/// The kind of an account row, for assertions that name it.
fn status_kind(status: &CheckCandidateStatus) -> &'static str {
    match status {
        CheckCandidateStatus::Offered => "offered",
        CheckCandidateStatus::Declined(_) => "declined",
        CheckCandidateStatus::OfferUndelivered { .. } => "undelivered",
        CheckCandidateStatus::NoRoomFits { .. } => "no room fits",
        CheckCandidateStatus::RefusedTwice { .. } => "refused twice",
        CheckCandidateStatus::HoldingRoom { .. } => "holding room",
        CheckCandidateStatus::RoomNotAnnounced { .. } => "not announced",
        CheckCandidateStatus::LinkStale { .. } => "link stale",
        CheckCandidateStatus::Away { .. } => "away",
        CheckCandidateStatus::Fits { .. } => "fits",
        CheckCandidateStatus::WaitsFor { .. } => "waits for",
        CheckCandidateStatus::NotOffered(_) => "not offered",
    }
}

/// A fleet of five linux machines, one per way a machine can stand with
/// waiting work: `refuser`, `small` and `stale` announce a room with no
/// memory available, `silent` is attached and has announced nothing, and
/// `private-away` (publicly `public-away`) is drained by an operator once
/// the work is queued, as `stale`'s link goes silent then ([`go_stale`]).
fn account_fleet(now: u64) -> Fleet {
    const GIB: u64 = 1 << 30;
    let pool = Fleet::default();
    let mut connections = HashMap::new();
    for id in ["refuser", "small", "silent", "stale", "private-away"] {
        let (id, mut entry) = fleet_entry_at(id, "linux", 0, &[], now);
        if id == "private-away" {
            entry.identity.display_name = "public-away".into();
        }
        connections.insert(id, entry);
    }
    *pool.lock_connections() = connections;
    for id in ["refuser", "small", "stale", "private-away"] {
        pool.announce_room_with_memory_for_test(id, 1, Some((0, 512 * GIB)));
    }
    pool.dispatcher_attached("silent", 1);
    pool
}

/// `stale`'s link has heard nothing for ten minutes, and an operator drains
/// `private-away`. A pin to a link already stale, or to a machine already
/// drained, is refused at once, so both happen once the work has queued.
fn go_stale(pool: &Fleet, now: u64) {
    let mut connections = pool.lock_connections();
    connections.get_mut("stale").unwrap().last_progress_unix_ms = now - 10 * 60_000;
    let away = connections.get_mut("private-away").unwrap();
    away.drained = true;
    away.drain_blockers = vec![cairn_common::executor_protocol::DrainBlocker {
        owner: "operator".into(),
        reason: "storage repair".into(),
        generation: 3,
        incarnation: None,
        estimate: None,
        admission_effect: Default::default(),
    }];
}

/// cairn/6499 under the dispatcher: one account says why a queued request
/// waits, machine by machine, whether it may spill or is pinned: a refusal
/// verbatim with the room it came under, a room that fits nothing with what
/// it is short of, a machine that has announced nothing (silence, never
/// "no room"), a stale link, and a machine away for now, named publicly.
/// A refusal under a room the machine has replaced is superseded, never the
/// reason now.
#[tokio::test]
async fn one_account_says_why_a_pinned_or_spillable_request_waits_on_each_machine() {
    const REFUSAL: &str = "owed 6.5 GiB + 2.0 GiB for this request > available 6.4 GiB";
    let policy = ActivePlacementPolicy::default_profile();
    let expected = [
        ("refuser", "declined"),
        ("small", "no room fits"),
        ("silent", "not announced"),
        ("stale", "link stale"),
        ("public-away", "away"),
    ];
    let phrases = [
        "declined by refuser (host pressure: owed 6.5 GiB + 2.0 GiB for this request > available 6.4 GiB; under its room #1)",
        "small: no announced room fits: needs ",
        "silent has not announced room (attached ",
        "stale: link stale since ",
        "away for now: public-away held from new admission: operator generation 3: storage repair",
    ];
    // Spill-eligible first, then pinned to each machine in turn.
    let pins: Vec<Option<&str>> = std::iter::once(None)
        .chain(
            ["refuser", "small", "silent", "stale", "private-away"]
                .into_iter()
                .map(Some),
        )
        .collect();
    for (case, pin) in pins.into_iter().enumerate() {
        let now = unix_time_ms();
        let pool = account_fleet(now);
        let mut request = spillable_request();
        request.request_id = format!("wait-{case}");
        request.pinned_executor_id = pin.map(str::to_string);
        request.wait_horizon_unix_ms = now + 600_000;
        let (entry, _waiter) = pool
            .queue
            .enqueue(
                queue::NewEntry {
                    request: request.clone(),
                    admission_kind: CellAdmissionKind::Command,
                    interactive: false,
                    execution_residency: None,
                    standing: false,
                },
                now,
            )
            .unwrap();
        let _deliveries = pool
            .join_dispatcher(&entry, &request, None, &policy)
            .await
            .unwrap_or_else(|outcome| panic!("{pin:?} waits rather than {outcome:?}"));
        go_stale(&pool, now);
        if matches!(pin, None | Some("refuser")) {
            // The machine refuses it under its room #1, in its own words.
            assert!(pool.queue.offer(&entry, "refuser", "refuser", 1, now));
            pool.queue.decline(
                &entry,
                "refuser",
                1,
                DeclineReason::Pressure,
                Some(REFUSAL.into()),
                None,
                now + 1,
            );
            pool.queue.note_decline_room(&entry, "refuser", 1);
            pool.dispatcher_declined(&entry, "refuser");
        }
        let Some(CheckDispatchPhase::Placing {
            declines,
            placement: Some(account),
            turn,
            ..
        }) = pool.queued_request_account(&request.request_id)
        else {
            panic!("{pin:?}: a queued request in the dispatcher has an account");
        };
        let kinds: HashMap<&str, &str> = account
            .machines
            .iter()
            .map(|machine| (machine.executor_name.as_str(), status_kind(&machine.status)))
            .collect();
        let held = crate::execution::checks::describe_queued_hold(
            &declines,
            Some(&account),
            turn.as_ref(),
        )
        .unwrap();
        for ((name, kind), phrase) in expected.iter().zip(phrases) {
            let named = pin.is_none_or(|pin| {
                pin == *name || (pin == "private-away" && *name == "public-away")
            });
            if named {
                assert_eq!(kinds.get(name), Some(kind), "{pin:?}: {held}");
                assert!(held.contains(phrase), "{pin:?}: {phrase} in {held}");
            } else {
                assert_eq!(kinds.get(name), None, "{pin:?}: {held}");
            }
        }
        assert!(!held.contains("private-away"), "{held}");
        if matches!(pin, None | Some("small")) {
            assert!(held.contains("of memory, 0.0 GiB available"), "{held}");
        }
        if pin.is_none() {
            // The same figures announced again move the seq, but the machine
            // would refuse the same need against them: the refusal stands.
            pool.announce_room_with_memory_for_test("refuser", 1, Some((0, 512 << 30)));
            let Some(CheckDispatchPhase::Placing {
                declines,
                placement: Some(account),
                ..
            }) = pool.queued_request_account(&request.request_id)
            else {
                panic!("still queued");
            };
            let refuser = account
                .machines
                .iter()
                .find(|machine| machine.executor_name == "refuser")
                .unwrap();
            assert_eq!(status_kind(&refuser.status), "declined");
            assert!(refuser.superseded.is_none());
            let held =
                crate::execution::checks::describe_queued_hold(&declines, Some(&account), None)
                    .unwrap();
            assert!(held.contains(phrases[0]), "{held}");

            // New figures replace the room the refusal came under.
            pool.announce_room_with_memory_for_test("refuser", 1, Some((0, 256 << 30)));
            let Some(CheckDispatchPhase::Placing {
                declines,
                placement: Some(account),
                ..
            }) = pool.queued_request_account(&request.request_id)
            else {
                panic!("still queued");
            };
            let refuser = account
                .machines
                .iter()
                .find(|machine| machine.executor_name == "refuser")
                .unwrap();
            assert_eq!(status_kind(&refuser.status), "no room fits");
            assert_eq!(
                refuser
                    .superseded
                    .as_ref()
                    .map(|old| (old.decline.room_seq, old.current_seq)),
                Some((Some(1), Some(3)))
            );
            let held =
                crate::execution::checks::describe_queued_hold(&declines, Some(&account), None)
                    .unwrap();
            assert!(!held.contains("declined by refuser"), "{held}");
            assert!(
                held.contains(&format!(
                    "superseded refusals: refuser (host pressure: {REFUSAL}; under its room #1), room #3 announced since"
                )),
                "{held}"
            );
        }
    }
}

/// Work waiting on a machine's update is told so while it waits, in the
/// update's own words, without promising a safe restart time while work
/// remains there; the refusal at its horizon records the machine as
/// update-pending with no bound. Before cairn/6382 work parked on bbs's
/// 68-minute drain left no decision anywhere.
#[tokio::test]
async fn a_wait_on_a_machines_update_records_an_unknown_safe_bound_while_work_remains() {
    let pool = Fleet::default();
    let now = unix_time_ms();
    let (executor_id, mut connection) = fleet_entry_at("bbs", "linux", 0, &[], now);
    connection.executor_build_id = Some("build-0759".into());
    Arc::make_mut(&mut connection.snapshot).granted_attempts =
        vec![cairn_common::executor_protocol::GrantedAttempt {
            request_id: "long-lane".into(),
            attempt_id: "a".into(),
        }];
    pool.lock_connections()
        .insert(executor_id.clone(), connection);
    pool.record_executor_artifact_build("x86_64-unknown-linux-gnu", Ok("build-1329".into()));
    pool.advance_self_update_rollout(now, |_| Ok(())).unwrap();
    assert!(pool.lock_connections()[&executor_id].update_hold);
    runtime_update_hold_fixture(pool.lock_connections().get_mut(&executor_id).unwrap(), true);

    let mut request = targeted_request("linux");
    request.repository = spillable_request().repository;
    request.executor = Some(ExecutorSelector {
        name: Some("bbs".into()),
        ..ExecutorSelector::default()
    });
    request.wait_horizon_unix_ms = unix_time_ms() + 400;
    let policy = ActivePlacementPolicy::default_profile();
    let (entry, _waiter) = pool
        .queue
        .enqueue(
            queue::NewEntry {
                request: request.clone(),
                admission_kind: CellAdmissionKind::Command,
                interactive: false,
                execution_residency: None,
                standing: false,
            },
            now,
        )
        .unwrap();
    let mut deliveries = Some(
        pool.join_dispatcher(&entry, &request, None, &policy)
            .await
            .expect("work naming an updating machine waits for it"),
    );
    let wait = pool.unplaced_wait(&request.request_id).unwrap();
    assert!(
        wait.contains(
            "away for now: bbs being emptied for an executor update (1 in flight, request long-lane"
        ),
        "{wait}"
    );
    assert!(wait.contains("cairn://executors/bbs"), "{wait}");
    assert!(wait.contains("no safe restart time"), "{wait}");
    assert!(!wait.contains("expected back by"), "{wait}");
    // An unmeasured scheduling budget never promises a safe restart.
    assert_eq!(
        pool.self_update.lock().unwrap().returns_by(&executor_id),
        None
    );

    let _joined = super::assignments::JoinedDispatcher {
        pool: pool.clone(),
        entry_id: entry.clone(),
    };
    pool.await_assignment(&entry, &request, None, &policy, &mut deliveries)
        .await
        .expect_err("the horizon arrives before the update ends");
    let bounds: Vec<Option<u64>> = pool
        .placements_naming("bbs")
        .into_iter()
        .flat_map(|decision| decision.rejected)
        .filter_map(|rejection| match rejection.reason {
            PlacementRejectionReason::UpdatePending { returns_by_unix_ms } => {
                Some(returns_by_unix_ms)
            }
            _ => None,
        })
        .collect();
    assert!(!bounds.is_empty(), "the refusal names the updating machine");
    assert!(bounds.iter().all(Option::is_none), "{bounds:?}");
}

/// An enrolled machine away within its link grace is a bounded wait for
/// work that names it, told as reconnecting; a pin or a public name waits
/// the same, and an expired absence is refused rather than waited on.
#[tokio::test]
async fn detached_targeted_requests_wait_only_within_the_return_bound() {
    let pool = Fleet::default();
    pool.declare_enrolled_remote("win", "windows-builder", "windows", "x86_64");
    let mut request = targeted_request("windows");
    request.repository = spillable_request().repository;
    // The absence is stamped when the enrollments are read, so `now` comes
    // after: the return bound is then within the grace of `now`.
    let detached = pool.unattached_enrolled_remotes();
    let now = unix_time_ms();
    let attached = HashMap::new();
    let survey = survey_candidates(&attached, &detached, &request, now)
        .expect("a detached enrollment is a bounded wait, not no matching executor");
    let win = survey
        .rejected
        .iter()
        .find(|rejection| rejection.executor_id == "win")
        .unwrap_or_else(|| panic!("the enrollment is named: {:?}", survey.rejected));
    assert!(
        matches!(win.reason,
            PlacementRejectionReason::Reconnecting { returns_by_unix_ms }
                if returns_by_unix_ms > now
                    && returns_by_unix_ms <= now + ENVIRONMENT_LEASE_GRACE_MS
        ),
        "{:?}",
        survey.rejected
    );
    request.wait_horizon_unix_ms = now + 600_000;
    let (entry, _waiter) = pool
        .queue
        .enqueue(
            queue::NewEntry {
                request: request.clone(),
                admission_kind: CellAdmissionKind::Command,
                interactive: false,
                execution_residency: None,
                standing: false,
            },
            now,
        )
        .unwrap();
    let _deliveries = pool
        .join_dispatcher(
            &entry,
            &request,
            None,
            &ActivePlacementPolicy::default_profile(),
        )
        .await
        .expect("the reconnect is waited for");
    let wait = pool.unplaced_wait(&request.request_id).unwrap();
    assert!(wait.contains("windows-builder"), "{wait}");
    assert!(wait.contains("reconnecting"), "{wait}");

    for named in [false, true] {
        // Each from a fresh enrollment: the previous case expired its absence.
        let pool = Fleet::default();
        pool.declare_enrolled_remote("win", "windows-builder", "windows", "x86_64");
        let mut pinned = request.clone();
        pinned.executor = None;
        if named {
            pinned.executor = Some(ExecutorSelector {
                name: Some("windows-builder".into()),
                ..Default::default()
            });
        } else {
            pinned.pinned_executor_id = Some("win".into());
        }
        assert!(
            survey_candidates(
                &HashMap::new(),
                &pool.unattached_enrolled_remotes(),
                &pinned,
                unix_time_ms()
            )
            .is_ok(),
            "named {named}: a detached enrollment within its grace is waited for"
        );
        pool.stage_link_absent("win", ENVIRONMENT_LEASE_GRACE_MS + 1_000);
        assert!(
            survey_candidates(
                &HashMap::new(),
                &pool.unattached_enrolled_remotes(),
                &pinned,
                unix_time_ms(),
            )
            .is_err(),
            "an expired detached pin must not wait forever"
        );
    }
}

#[test]
fn check_dispatch_phase_is_none_once_the_execution_settles() {
    let pool = Fleet::default();
    let leader = ("leader".to_string(), "leader-attempt".to_string());
    let (own, _) = dispatched_check(&pool, &leader);
    let entry = enqueue_owned_entry(&pool, &leader);
    assert!(pool.queue.offer(&entry, "fedora", "fedora", 3, 2_000));
    assert!(pool.queue.settle(
        &leader,
        None,
        queue::TerminalState::Completed,
        CellOutcome::Cancelled {
            request_id: leader.0.clone(),
            attempt_id: leader.1.clone(),
        },
        3_000,
    ));
    assert_eq!(pool.check_dispatch_phase(&own), None);
    assert_eq!(
        pool.check_dispatch_phase(&("unknown:check-0".into(), "unknown:check-0".into())),
        None
    );
}

/// cairn/7318: a request that waits costs the profile store nothing. Its
/// needs on a machine are worked out once, and are worked out again only when
/// a lesson names something they were derived from, so one command finishing
/// leaves every other parked command's needs standing.
#[tokio::test]
async fn a_parked_requests_needs_are_worked_out_once_and_only_its_own_lessons_redo_them() {
    const GIB: u64 = 1024 * 1024 * 1024;
    let db = Arc::new(crate::storage::migrated_test_db("needs-worked-out-once.db").await);
    let capabilities = capabilities_with_cores(16);
    let identity = |key: &str| CommandResourceIdentity {
        version: 1,
        key: key.into(),
        suite: None,
    };
    let (x, y) = (identity("needs-x"), identity("needs-y"));
    let request_for = |identity: &CommandResourceIdentity| {
        let mut request = provenance_request();
        request.command_resource_identity = Some(identity.clone());
        request
    };
    let (request_x, request_y) = (request_for(&x), request_for(&y));
    let (plan_x, plan_y) = (
        ReservationPlan::new(db.clone(), &request_x, None),
        ReservationPlan::new(db.clone(), &request_y, None),
    );
    let duration = resource_profiles::DurationContext {
        class: request_x.command_class,
        warmth: ExecutionWarmth::Cold,
        now_unix_ms: NOW,
    };
    macro_rules! place {
        ($plan:expr, $request:expr) => {
            $plan.resolve_for($request, "device", "executor", &capabilities, 0, duration)
        };
    }

    // Queued: one resolution each.
    let unmeasured = place!(&plan_x, &request_x).await;
    place!(&plan_y, &request_y).await;
    assert_eq!(plan_x.take_needs_worked_out(), 1);
    assert_eq!(plan_y.take_needs_worked_out(), 1);
    assert_eq!(
        unmeasured.reservation.source,
        ResourceReservationSource::Unmeasured
    );

    // Parked through two hundred placement passes: nothing is worked out and
    // the store is not read for needs.
    let before = db.read_statement_count();
    for _ in 0..200 {
        assert_eq!(
            place!(&plan_x, &request_x).await.reservation,
            unmeasured.reservation
        );
        place!(&plan_y, &request_y).await;
    }
    assert_eq!(plan_x.take_needs_worked_out(), 0);
    assert_eq!(plan_y.take_needs_worked_out(), 0);
    assert_eq!(
        db.read_statement_count(),
        before,
        "a pass that learned nothing reads nothing"
    );

    // X finishes on a machine: X's needs are worked out again and see it, and
    // Y's are untouched.
    let context = ReservationPlan::context_for("device", "executor", &capabilities);
    resource_profiles::seed_memory_profile_for_test(db.clone(), &x, &context, 6 * GIB).await;
    let learned = place!(&plan_x, &request_x).await;
    place!(&plan_y, &request_y).await;
    assert_eq!(plan_x.take_needs_worked_out(), 1);
    assert_eq!(plan_y.take_needs_worked_out(), 0);
    assert_eq!(
        learned.reservation.source,
        ResourceReservationSource::Learned
    );
    assert!(learned.reservation.memory_bytes >= 6 * GIB);

    // A write to rows no need reads (contention, durations) redoes nothing.
    drop(super::profile_reads::Writing::apart_from_needs(&db));
    place!(&plan_x, &request_x).await;
    assert_eq!(plan_x.take_needs_worked_out(), 0);

    // A different machine of the same request is its own context.
    plan_x
        .resolve_for(&request_x, "device", "other", &capabilities, 0, duration)
        .await;
    assert_eq!(plan_x.take_needs_worked_out(), 1);
    assert_eq!(plan_x.needs_held(), 2);
}

fn provenance_request() -> CellRequest {
    CellRequest {
        request_id: "provenance".into(),
        attempt_id: "attempt".into(),
        project_id: "p".into(),
        repository: RepositoryLocator::ColocatedPath {
            project_id: "p".into(),
            repository_id: "p".into(),
            absolute_path: "/repo".into(),
        },
        base_commit: "base".into(),
        command: "cargo test --workspace".into(),
        command_class: cairn_common::executor_protocol::CellCommandClass::CargoTest,
        placement_work_class: cairn_common::executor_protocol::PlacementWorkClass::AgentSessions,
        owner: None,
        cwd: String::new(),
        env: Vec::new(),
        priority: CellPriority::ReviewCheck,
        wait_horizon_unix_ms: unix_time_ms() + 5_000,
        waiting_since_unix_ms: 0,
        timeout_ms: 1_000,
        mutation_policy: MutationPolicy::PureVerdict,
        requesting_job_id: None,
        affinity_key: None,
        executor: None,
        pinned_executor_id: None,
        placement_mobility: Default::default(),
        verdict_platforms: Vec::new(),
        avoided_executor_ids: Vec::new(),
        project_requirements: Default::default(),
        command_resource_identity: Some(CommandResourceIdentity {
            version: cairn_common::executor_protocol::COMMAND_RESOURCE_IDENTITY_VERSION,
            key: "check:rust".into(),
            suite: None,
        }),
        resource_reservation: Default::default(),
        learned_estimate: None,
        scratch_sync: None,
        home_affinity: None,
        publication: None,
    }
}

/// A modest declaration is carried through untouched; the clamp is a ceiling,
/// not a rewrite.
#[test]
fn a_declaration_within_capacity_is_left_alone() {
    assert_eq!(
        clamp_declared_concurrency(1, &capabilities_with_cores(12)),
        1
    );
}

/// An executor that advertises no cores must still yield a runnable
/// declaration. Zero is rejected by the runtime policy as an invalid
/// reservation, which would strand every whole-machine check.
#[test]
fn a_coreless_advertisement_still_yields_a_runnable_declaration() {
    assert_eq!(
        clamp_declared_concurrency(
            ResourceReservation::WHOLE_MACHINE_CONCURRENCY,
            &capabilities_with_cores(0)
        ),
        2
    );
}

fn resolved_process_batch(
    timeouts: Vec<Option<u32>>,
    sequential: bool,
    stop_on_error: bool,
) -> ResolvedRunBatch {
    ResolvedRunBatch {
        execution_residency: None,
        request: crate::mcp::types::McpCallbackRequest {
            answer_bound_ms: None,
            thread_id: None,
            cwd: "/tmp".into(),
            run_id: None,
            tool: "run".into(),
            payload: serde_json::Value::Null,
            tool_use_id: None,
            origin: None,
        },
        run_context: None,
        resolved: timeouts
            .into_iter()
            .enumerate()
            .map(|(index, timeout)| {
                (
                    format!("command-{index}"),
                    Ok(RunSpec::Shell {
                        command: "true".into(),
                        timeout,
                        args: Vec::new(),
                    }),
                )
            })
            .collect(),
        tool_use_id: "tool-use".into(),
        stop_on_error,
        originally_sequential: sequential,
        node_scratch: None,
    }
}

#[test]
fn residency_acquisition_phases_are_request_scoped_progress() {
    let pool = Fleet::default();
    let (tx, _rx) = mpsc::unbounded_channel();
    let generation = pool.attach_executor(tx);
    let request_id = "residency-acquire:job";

    for state in [
        ExecutorSubstrateState::SlotAdoption,
        ExecutorSubstrateState::DispatchPreparing,
    ] {
        let evidence =
            ExecutorSubstrateEvidence::without_queue(state, unix_time_ms(), unix_time_ms());
        assert!(pool.set_executor_snapshot(
            COLOCATED_EXECUTOR_ID,
            generation,
            FleetSnapshot {
                in_flight_residency_operations: HashMap::from([(
                    request_id.into(),
                    evidence.clone(),
                )]),
                ..FleetSnapshot::default()
            },
            ExecutorSubstrateReport::default(),
        ));
        assert_eq!(
            pool.request_substrate_hold(
                COLOCATED_EXECUTOR_ID,
                generation,
                request_id,
                RESIDENCY_ACQUIRE_ATTEMPT_ID,
            ),
            Some(evidence),
        );
    }
}

#[test]
fn executor_level_hold_is_level_readable_for_late_and_concurrent_waiters() {
    let pool = Fleet::default();
    let (tx, _rx) = mpsc::unbounded_channel();
    let generation = pool.attach_executor(tx);
    let since = unix_time_ms().saturating_sub(10_000);
    let last_progress = since + 5_000;
    let reported = ExecutorSubstrateEvidence::without_queue(
        ExecutorSubstrateState::InitialStorageSweep,
        since,
        last_progress,
    );
    assert!(pool.set_executor_snapshot(
        COLOCATED_EXECUTOR_ID,
        generation,
        FleetSnapshot {
            substrate_state: Some(reported.clone()),
            ..FleetSnapshot::default()
        },
        ExecutorSubstrateReport::default(),
    ));

    assert_eq!(
        pool.request_substrate_hold(COLOCATED_EXECUTOR_ID, generation, "late-first", "attempt"),
        Some(reported.clone())
    );
    assert_eq!(
        pool.request_substrate_hold(COLOCATED_EXECUTOR_ID, generation, "late-second", "attempt"),
        Some(reported)
    );

    let accounting_reported = ExecutorSubstrateEvidence::without_queue(
        ExecutorSubstrateState::StorageAccounting,
        since + 6_000,
        last_progress + 6_000,
    );
    assert!(pool.set_executor_snapshot(
        COLOCATED_EXECUTOR_ID,
        generation,
        FleetSnapshot {
            substrate_state: Some(accounting_reported.clone()),
            ..FleetSnapshot::default()
        },
        ExecutorSubstrateReport::default(),
    ));
    assert_eq!(
        pool.request_substrate_hold(
            COLOCATED_EXECUTOR_ID,
            generation,
            "accounting-first",
            "attempt",
        ),
        Some(accounting_reported.clone())
    );
    assert_eq!(
        pool.request_substrate_hold(
            COLOCATED_EXECUTOR_ID,
            generation,
            "accounting-second",
            "attempt",
        ),
        Some(accounting_reported)
    );
}

/// What the fixture below hands back: the fleet, the executor generation, the
/// shared result identity, the leader's identity and result channel, and one
/// identity/channel pair per coalesced follower.
type ColocatedLeaderLink = (
    Fleet,
    u64,
    CoalescingKey,
    RequestIdentity,
    oneshot::Receiver<CellOutcome>,
    Vec<(RequestIdentity, oneshot::Receiver<CoalescedDelivery>)>,
);

/// Wire a colocated connection whose leader is executing, with `followers`
/// extra coalesced subscribers attached to the same result identity.
///
/// This is the incident's shape at the seam: an executor that stays attached
/// and holds a live child process while the runner stops hearing from it.
fn colocated_link_with_executing_leader(followers: usize) -> ColocatedLeaderLink {
    let pool = Fleet::default();
    let (sender, _executor) = mpsc::unbounded_channel();
    std::mem::forget(_executor);
    let generation = pool.attach_executor(sender);
    let leader: RequestIdentity = ("leader-request".into(), "leader-attempt".into());
    let result_identity = CoalescingKey::new(result_identity(), leader.clone());

    let leader_rx = claim_entry(
        &pool,
        &leader,
        COLOCATED_EXECUTOR_ID,
        generation,
        Some("job"),
    );

    let mut subscribers = HashMap::new();
    let mut follower_waiters = Vec::new();
    for index in 0..followers {
        let identity: RequestIdentity = (
            format!("follower-request-{index}"),
            "follower-attempt".into(),
        );
        let (tx, rx) = oneshot::channel();
        subscribers.insert(
            identity.clone(),
            CoalescedSubscriber {
                waiter: tx,
                priority: CellPriority::ReviewCheck,
                requesting_job_id: None,
            },
        );
        follower_waiters.push((identity, rx));
    }
    {
        let mut registry = pool.in_flight.lock().unwrap();
        registry.open(
            result_identity.clone(),
            PlacementDemand::default(),
            subscribers,
        );
    }
    pool.coalesced_leaders
        .lock()
        .unwrap()
        .insert(leader.clone());

    // A live child process is what latches `execution_started` in
    // `await_coalesced`, which is precisely the state that has no deadline
    // left to expire against.
    assert!(pool.set_executor_snapshot(
        COLOCATED_EXECUTOR_ID,
        generation,
        FleetSnapshot {
            executing_requests: vec![ExecutingCellRequest {
                command_resource_identity: None,
                executor_id: COLOCATED_EXECUTOR_ID.into(),
                cell_id: "cell-1".into(),
                request_id: leader.0.clone(),
                attempt_id: leader.1.clone(),
                owner: None,
                command_class: Default::default(),
                command: "bun run test".into(),
                started_at_unix_ms: unix_time_ms().saturating_sub(600_000),
                process_ids: vec![4242],
                priority: Some(CellPriority::ReviewCheck),
                subscriber_count: followers,
                resource_reservation: Default::default(),
                learned_estimate: None,
                live_usage: None,
                build_jobs: None,
                adopted: None,
                memory_phase: None,
                standalone: false,
            }],
            ..FleetSnapshot::default()
        },
        ExecutorSubstrateReport::default(),
    ));

    (
        pool,
        generation,
        result_identity,
        leader,
        leader_rx,
        follower_waiters,
    )
}

/// The acceptance criterion "bound on silence, not on duration". A cell that
/// has been executing for ten minutes is healthy as long as the link keeps
/// reporting, and the executor's heartbeat guarantees it does.
#[test]
fn a_long_running_cell_on_a_reporting_link_is_never_remediated() {
    let (pool, _generation, _result_identity, _leader, _leader_rx, _followers) =
        colocated_link_with_executing_leader(0);

    assert_eq!(
        pool.assess_colocated_link(unix_time_ms(), LinkStallBounds::default()),
        LinkRemediation::Healthy
    );
}

/// The two thresholds must stay distinct: subscribers see `ConnectedStalled`
/// for more than a minute before the supervisor takes the link away, which is
/// what keeps remediation a last resort rather than a hair trigger.
#[test]
fn connected_stalled_is_observable_well_before_remediation_fires() {
    let (pool, generation, ..) = colocated_link_with_executing_leader(0);
    let stalled_at = unix_time_ms() + EXECUTOR_PROGRESS_FRESHNESS_MS + 1;

    assert_eq!(
        deadline_evidence(
            stalled_at,
            pool.lock_connections()
                .get(COLOCATED_EXECUTOR_ID)
                .unwrap()
                .last_progress_unix_ms,
            ExecutorSubstrateEvidence::without_queue(
                ExecutorSubstrateState::ExecutionRunning,
                stalled_at,
                stalled_at,
            ),
        )
        .state,
        ExecutorSubstrateState::ConnectedStalled
    );
    assert_eq!(
        pool.assess_colocated_link(stalled_at, LinkStallBounds::default()),
        LinkRemediation::Healthy
    );
    assert!(matches!(
        pool.assess_colocated_link(
            unix_time_ms() + EXECUTOR_LINK_STALL_REMEDIATION_MS + 1,
            LinkStallBounds::default(),
        ),
        LinkRemediation::Bounce {
            generation: bounced,
            cause: LinkStallCause::Silence { .. },
            ..
        } if bounced == generation
    ));
}

/// The 10-16 misreading, closed. The reader for this link has not run for
/// 200 s, so the runner heard nothing in that time and charges the machine
/// with none of it: placement names the runner as the gap, remediation does
/// not bounce a healthy child, and the status every page shows stays
/// online. The reader resuming hands the peer a fresh window rather than
/// declaring it silent on the spot.
#[test]
fn a_link_the_runner_was_not_listening_to_is_never_charged_with_silence() {
    let (pool, generation, ..) = colocated_link_with_executing_leader(0);
    let clock = pool.link_clock(COLOCATED_EXECUTOR_ID, generation).unwrap();
    clock.stage(200_000, 200_000);
    let now = unix_time_ms();
    let request = spillable_request();

    let connections = pool.lock_connections();
    let entry = connections.get(COLOCATED_EXECUTOR_ID).unwrap();
    assert!(matches!(
        candidate_rejection(entry, PlacementScope::of(&request), now),
        Some(PlacementRejectionReason::RunnerNotListening {
            reader_silence_ms: 200_000..,
            ..
        })
    ));
    assert_eq!(
        executor_link_status(entry, now),
        ExecutorHealthStatus::Online
    );
    drop(connections);
    assert_eq!(
        pool.assess_colocated_link(now, LinkStallBounds::default()),
        LinkRemediation::Healthy
    );
    assert_eq!(pool.peer_silence_ms(COLOCATED_EXECUTOR_ID, now), None);

    clock.reader_ticked();
    let connections = pool.lock_connections();
    let entry = connections.get(COLOCATED_EXECUTOR_ID).unwrap();
    assert_eq!(
        judge_link(entry, now, EXECUTOR_PROGRESS_FRESHNESS_MS),
        link_clock::LinkJudgement::Fresh,
        "a resumed reader gives the peer a full window before judging it"
    );
}

/// And a reader that was listening the whole time does charge the silence.
#[test]
fn a_link_silent_while_the_runner_listened_is_stale_and_bounced() {
    let (pool, generation, ..) = colocated_link_with_executing_leader(0);
    let clock = pool.link_clock(COLOCATED_EXECUTOR_ID, generation).unwrap();
    clock.stage(EXECUTOR_LINK_STALL_REMEDIATION_MS + 1_000, 0);
    let now = unix_time_ms();
    let request = spillable_request();
    {
        let connections = pool.lock_connections();
        let entry = connections.get(COLOCATED_EXECUTOR_ID).unwrap();
        assert!(matches!(
            candidate_rejection(entry, PlacementScope::of(&request), now),
            Some(PlacementRejectionReason::ConnectionStale { .. })
        ));
    }
    assert!(matches!(
        pool.assess_colocated_link(now, LinkStallBounds::default()),
        LinkRemediation::Bounce {
            cause: LinkStallCause::Silence {
                pump_silence_ms: Some(0..=1_000),
                ..
            },
            ..
        }
    ));
}

#[test]
fn an_unattached_colocated_link_is_not_a_link_to_bounce() {
    let pool = Fleet::default();

    assert_eq!(
        pool.assess_colocated_link(
            unix_time_ms() + EXECUTOR_LINK_STALL_REMEDIATION_MS * 10,
            LinkStallBounds::default(),
        ),
        LinkRemediation::Healthy
    );
    assert_eq!(
        pool.abandon_stalled_colocated_link(
            COLOCATED_EXECUTOR_ID,
            1,
            LinkStallCause::Silence {
                silence_ms: 0,
                pump_silence_ms: Some(0),
            },
        ),
        StalledLinkAbandonment::default()
    );
}

/// The regression guard for the whole issue: a subscriber whose execution had
/// already started has no deadline left to expire against, so if remediation
/// does not resolve it explicitly the link reset converts a stalled link into
/// an indefinite hold.
#[tokio::test]
async fn abandoning_a_stalled_link_resolves_started_subscribers_typed() {
    let (pool, generation, _result_identity, leader, leader_rx, mut followers) =
        colocated_link_with_executing_leader(1);
    let (follower_identity, follower_rx) = followers.pop().unwrap();

    // Deadline already in the past: only the `execution_started` latch is
    // keeping this subscriber alive, exactly as in the incident.
    let waiting = tokio::spawn({
        let pool = pool.clone();
        let follower_identity = follower_identity.clone();
        async move {
            pool.await_coalesced(follower_identity, unix_time_ms(), follower_rx)
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        !waiting.is_finished(),
        "a started subscriber must not expire against its own deadline"
    );

    let abandonment = pool.abandon_stalled_colocated_link(
        COLOCATED_EXECUTOR_ID,
        generation,
        LinkStallCause::Silence {
            silence_ms: EXECUTOR_LINK_STALL_REMEDIATION_MS + 1,
            pump_silence_ms: Some(EXECUTOR_LINK_STALL_REMEDIATION_MS + 1),
        },
    );
    assert!(abandonment.disconnected);
    assert_eq!(abandonment.abandoned_attempts, 1);
    // The fleet leaves the reset row to the supervisor that decided it.
    assert!(pool
        .runner_events()
        .recent()
        .iter()
        .all(|event| { event.kind != runner_events::RunnerEventKind::ExecutorLinkReset }));

    // Resolution arrives as a published coalesced outcome rather than a
    // subscriber-side error: the attempt really was decided, and the verdict
    // it was decided with is retryable.
    let outcome = tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .expect("a subscriber must never hold indefinitely across a link reset")
        .unwrap();
    let resolved = match outcome {
        Ok(coalesced) => coalesced.outcome,
        Err(CoalescedWaitError::Refused(outcome)) => *outcome,
        Err(error) => panic!("a link reset must resolve with an outcome, got {error:?}"),
    };
    assert!(
        matches!(
            resolved,
            CellOutcome::Unavailable {
                reason: CellUnavailableReason::ExecutorUnavailable,
                ..
            }
        ),
        "a subscriber must resolve retryable across a link reset, got {resolved:?}"
    );

    // The leader's own attempt resolves retryable through the same teardown,
    // and every registry the connection owned is left empty for it.
    assert!(matches!(
        leader_rx.await.unwrap(),
        CellOutcome::Unavailable {
            reason: CellUnavailableReason::ExecutorUnavailable,
            ..
        }
    ));
    assert!(pool.queue.len() == 0);
    assert!(pool.pending_residency.lock().unwrap().is_empty());
    assert!(pool
        .pending_materialization_reads
        .lock()
        .unwrap()
        .is_empty());
    let registry = pool.in_flight.lock().unwrap();
    assert!(registry.by_key.is_empty());
    assert!(registry.subscriber_keys.is_empty());
    drop(registry);
    assert!(!pool.coalesced_leaders.lock().unwrap().contains(&leader));

    // Waiters see a recovering environment rather than a failing one, and the
    // link is free for the supervisor's replacement generation.
    assert_eq!(
        pool.colocated_substrate().unwrap().state,
        ExecutorSubstrateState::SupervisorRespawning
    );
    assert!(placement::substrate_is_working(
        pool.colocated_substrate().unwrap().state
    ));
    assert_eq!(pool.executor_generation(), None);
}

/// A verdict formed about one connection must never be executed against its
/// replacement. Without the generation fence, a bounce that races a reattach
/// tears down the healthy link that just arrived.
#[test]
fn a_stale_verdict_cannot_bounce_the_replacement_link() {
    let (pool, stale_generation, ..) = colocated_link_with_executing_leader(0);
    assert!(matches!(
        pool.assess_colocated_link(
            unix_time_ms() + EXECUTOR_LINK_STALL_REMEDIATION_MS + 1,
            LinkStallBounds::default(),
        ),
        LinkRemediation::Bounce { .. }
    ));

    let (sender, replacement_executor) = mpsc::unbounded_channel();
    let replacement = pool.attach_executor(sender);
    assert!(replacement > stale_generation);

    assert!(
        !pool
            .abandon_stalled_colocated_link(
                COLOCATED_EXECUTOR_ID,
                stale_generation,
                LinkStallCause::Silence {
                    silence_ms: EXECUTOR_LINK_STALL_REMEDIATION_MS + 1,
                    pump_silence_ms: Some(EXECUTOR_LINK_STALL_REMEDIATION_MS + 1),
                },
            )
            .disconnected
    );
    assert_eq!(pool.executor_generation(), Some(replacement));
    assert_eq!(
        pool.assess_colocated_link(unix_time_ms(), LinkStallBounds::default()),
        LinkRemediation::Healthy
    );
    drop(replacement_executor);
}

/// A restage is a change of advertisement. Republishing the same one, which
/// every endpoint refresh and colocated reset may do, records nothing.
#[test]
fn only_a_changed_listener_advertisement_is_recorded_as_a_restage() {
    let pool = Fleet::default();
    let advertisement = |cert: &str| ExecutorListenerAdvertisement {
        endpoints: vec!["wss://runner:3850/api/executor".into()],
        cert_sha256: vec![cert.into()],
    };
    pool.publish_executor_listener(advertisement("aa"));
    pool.publish_executor_listener(advertisement("aa"));
    pool.publish_executor_listener(advertisement("bb"));
    let restages: Vec<_> = pool
        .runner_events()
        .recent()
        .into_iter()
        .filter(|event| event.kind == runner_events::RunnerEventKind::ListenerRestaged)
        .collect();
    assert_eq!(restages.len(), 2);
    assert_eq!(
        restages[0].detail["previous_cert_sha256"],
        serde_json::Value::Null
    );
    assert_eq!(restages[1].detail["previous_cert_sha256"][0], "aa");
    assert_eq!(restages[1].detail["cert_sha256"][0], "bb");
}

#[test]
fn a_colocated_substrate_failure_is_recorded_once_per_declaration() {
    let pool = Fleet::default();
    pool.declare_colocated_substrate_failure("spawn failed: no such file".into());
    pool.declare_colocated_permanent_refusal("protocol mismatch".into());
    let failures: Vec<_> = pool
        .runner_events()
        .recent()
        .into_iter()
        .filter(|event| event.kind == runner_events::RunnerEventKind::ExecutorSubstrateFailed)
        .collect();
    assert_eq!(failures.len(), 2);
    assert_eq!(
        failures[0].executor_id.as_deref(),
        Some(COLOCATED_EXECUTOR_ID)
    );
    assert_eq!(
        failures[0].cause.as_deref(),
        Some("spawn failed: no such file")
    );
    assert_eq!(failures[0].detail["state"], "supervisor_respawning");
    assert_eq!(failures[1].detail["state"], "permanent_refusal");
    assert_eq!(failures[1].detail["failed_attempts"], 2);
    assert_eq!(
        failures[1].detail["failing_since_ms"],
        failures[0].detail["failing_since_ms"]
    );
}

#[test]
fn a_stall_cause_carries_its_measurements_as_fields() {
    let frozen = LinkStallCause::TelemetryFrozen {
        frozen_ms: 1_009_658,
    }
    .detail();
    assert_eq!(frozen["stall_kind"], "telemetry_frozen");
    assert_eq!(frozen["frozen_ms"], 1_009_658);
    let silent = LinkStallCause::Silence {
        silence_ms: 70_000,
        pump_silence_ms: None,
    }
    .detail();
    assert_eq!(silent["stall_kind"], "silence");
    assert_eq!(silent["silence_ms"], 70_000);
    assert_eq!(silent["pump_silence_ms"], serde_json::Value::Null);
}

/// Put this connection's published facts that far in the past while leaving
/// the link itself beating normally: the CAIRN-4362 specimen shape. Both
/// clocks agree, as they do on a host that has not slept.
fn facts_last_measured(pool: &Fleet, age_ms: u64) {
    let mut connections = pool.lock_connections();
    let entry = connections.get_mut(COLOCATED_EXECUTOR_ID).unwrap();
    entry.last_progress_unix_ms = unix_time_ms();
    Arc::make_mut(&mut entry.advertisement).liveness_observed_at_unix_ms =
        Some(unix_time_ms().saturating_sub(age_ms));
    entry.link_clock.stage_facts_recomputed(age_ms);
}

/// The specimen this remediation exists for: three machines refused for
/// placement for a quarter of an hour while every heartbeat arrived on
/// time. Silence remediation cannot see it, because there is no silence.
#[test]
fn a_link_whose_facts_froze_while_it_kept_beating_is_bounced() {
    let (pool, generation, ..) = colocated_link_with_executing_leader(0);
    facts_last_measured(&pool, EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS + 1);

    assert!(matches!(
        pool.assess_colocated_link(unix_time_ms(), LinkStallBounds::default()),
        LinkRemediation::Bounce {
            generation: bounced,
            cause: LinkStallCause::TelemetryFrozen { .. },
            ..
        } if bounced == generation
    ));
}

/// Say that this connection's executor has reported which phase its refresh
/// pass is stuck in, which is what a machine naming its own wedge looks
/// like on the wire.
fn refresh_blocked_in(pool: &Fleet, phase: LivenessRefreshPhase) {
    let mut connections = pool.lock_connections();
    let entry = connections.get_mut(COLOCATED_EXECUTOR_ID).unwrap();
    let began = unix_time_ms().saturating_sub(LIVENESS_REFRESH_BOUND_MS);
    Arc::make_mut(&mut entry.advertisement).facts_stale_refresh_blocked =
        Some(LivenessRefreshBlocked {
            phase,
            pass_started_unix_ms: began,
            phase_entered_unix_ms: began,
        });
}

/// The bounce is not free: it ends the executor daemon and every batch
/// running on the machine (CAIRN-4417). Spending that on a machine that is
/// telling us exactly what it is stuck in trades a reported fault for an
/// unreported one, so an explained freeze stays a health verdict and the
/// link keeps its work.
#[test]
fn a_fact_freeze_the_executor_explains_is_not_bounced() {
    let (pool, generation, ..) = colocated_link_with_executing_leader(0);
    facts_last_measured(&pool, EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS * 10);
    refresh_blocked_in(&pool, LivenessRefreshPhase::Snapshot);

    assert_eq!(
        pool.assess_colocated_link(unix_time_ms(), LinkStallBounds::default()),
        LinkRemediation::Healthy
    );
    assert_eq!(
        pool.managed_telemetry_stall_ms(COLOCATED_EXECUTOR_ID, generation, unix_time_ms()),
        None,
        "a machine that named its wedge has already done what the bounce exists to provoke"
    );
}

/// Explaining a freeze is not a way to be trusted through it. The facts are
/// exactly as stale as they were, every surface that judges freshness still
/// says so, and only the remedy changed.
#[test]
fn an_explained_freeze_is_still_reported_as_stale_telemetry() {
    let (pool, ..) = colocated_link_with_executing_leader(0);
    facts_last_measured(&pool, EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS * 10);
    refresh_blocked_in(&pool, LivenessRefreshPhase::Snapshot);

    let health = pool.executor_health(unix_time_ms()).remove(0);
    assert!(health.telemetry_stale);
    assert_eq!(health.status, ExecutorHealthStatus::Online);
    assert_eq!(
        health
            .advertisement
            .facts_stale_refresh_blocked
            .map(|blocked| blocked.phase),
        Some(LivenessRefreshPhase::Snapshot),
        "the phase has to survive to the surface an operator reads"
    );
}

/// And an explanation buys nothing against silence. A machine that reported
/// a wedge and then stopped answering is gone, whatever its last beat said,
/// or the marker becomes a way to be unbounceable.
#[test]
fn silence_outranks_an_explained_fact_freeze() {
    let (pool, ..) = colocated_link_with_executing_leader(0);
    facts_last_measured(&pool, EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS * 10);
    refresh_blocked_in(&pool, LivenessRefreshPhase::Snapshot);

    assert!(matches!(
        pool.assess_colocated_link(
            unix_time_ms() + EXECUTOR_LINK_STALL_REMEDIATION_MS + 1,
            LinkStallBounds::default(),
        ),
        LinkRemediation::Bounce {
            cause: LinkStallCause::Silence { .. },
            ..
        }
    ));
}

/// Frozen facts are a health verdict long before they are grounds to take a
/// machine away. Inside the bound the link keeps its work.
#[test]
fn frozen_facts_inside_the_bound_are_a_verdict_rather_than_a_bounce() {
    let (pool, ..) = colocated_link_with_executing_leader(0);
    facts_last_measured(&pool, EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS - 1);

    assert_eq!(
        pool.assess_colocated_link(unix_time_ms(), LinkStallBounds::default()),
        LinkRemediation::Healthy
    );
}

/// Silence is the stronger statement, and the two send an operator to
/// different places. A link nobody has heard from must never be reported as
/// a machine with a telemetry problem.
#[test]
fn silence_outranks_frozen_facts_when_both_are_past_their_bounds() {
    let (pool, ..) = colocated_link_with_executing_leader(0);
    facts_last_measured(&pool, 0);

    assert!(matches!(
        pool.assess_colocated_link(
            unix_time_ms() + EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS + 1,
            LinkStallBounds::default(),
        ),
        LinkRemediation::Bounce {
            cause: LinkStallCause::Silence { .. },
            ..
        }
    ));
}

/// A machine that never claimed a recompute clock cannot have one stop.
/// `attach_executor` advertises none, which is exactly what an executor
/// built before the split stamp sends.
#[test]
fn a_link_that_publishes_no_recompute_stamp_is_never_bounced_for_it() {
    let (pool, ..) = colocated_link_with_executing_leader(0);

    assert_eq!(
        pool.assess_colocated_link(
            unix_time_ms() + EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS * 10,
            LinkStallBounds {
                silence_ms: u64::MAX,
                ..LinkStallBounds::default()
            },
        ),
        LinkRemediation::Healthy
    );
}

/// What the SSH carrier's poll reads. The age is the executor's own
/// recompute stamp, so the carrier and the colocated supervisor judge the
/// same fact.
#[test]
fn a_managed_link_reports_how_long_its_facts_have_been_frozen() {
    let (pool, generation, ..) = colocated_link_with_executing_leader(0);
    facts_last_measured(&pool, EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS + 1);

    let stall = pool
        .managed_telemetry_stall_ms(COLOCATED_EXECUTOR_ID, generation, unix_time_ms())
        .expect("an attached link with a frozen recompute stamp reports its age");
    assert!(stall > EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS);
}

/// A link that has also gone silent belongs to silence remediation, which
/// acts sooner and says something different. Reporting a telemetry stall
/// here would let the carrier recycle it under the wrong name.
#[test]
fn a_silent_managed_link_reports_no_telemetry_stall() {
    let (pool, generation, ..) = colocated_link_with_executing_leader(0);
    facts_last_measured(&pool, EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS + 1);

    assert_eq!(
        pool.managed_telemetry_stall_ms(
            COLOCATED_EXECUTOR_ID,
            generation,
            unix_time_ms() + EXECUTOR_LINK_STALL_REMEDIATION_MS + 1,
        ),
        None
    );
}

/// The same generation fence the silence path carries: a reattach that
/// lands between assessment and action owns the link now.
#[test]
fn a_superseded_generation_reports_no_telemetry_stall() {
    let (pool, generation, ..) = colocated_link_with_executing_leader(0);
    facts_last_measured(&pool, EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS + 1);

    assert_eq!(
        pool.managed_telemetry_stall_ms(COLOCATED_EXECUTOR_ID, generation + 1, unix_time_ms(),),
        None
    );
}

/// A heartbeat the reader has received but ingest has not applied yet
/// carries the executor's newer facts. While one waits, the facts on
/// record are the runner's backlog, not the executor's freeze, and neither
/// remediation path may bounce on them; once ingest applies it, the
/// judgement is the executor's again.
#[test]
fn an_ingest_backlog_is_never_read_as_frozen_telemetry() {
    let (pool, generation, ..) = colocated_link_with_executing_leader(0);
    facts_last_measured(&pool, EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS + 1);
    let clock = pool.link_clock(COLOCATED_EXECUTOR_ID, generation).unwrap();
    clock.stage(0, 0);
    let waiting = clock.heartbeat_received();

    assert_eq!(
        pool.assess_colocated_link(unix_time_ms(), LinkStallBounds::default()),
        LinkRemediation::Healthy
    );
    assert_eq!(
        pool.managed_telemetry_stall_ms(COLOCATED_EXECUTOR_ID, generation, unix_time_ms()),
        None
    );

    clock.heartbeat_applied(waiting);
    assert!(matches!(
        pool.assess_colocated_link(unix_time_ms(), LinkStallBounds::default()),
        LinkRemediation::Bounce {
            cause: LinkStallCause::TelemetryFrozen { .. },
            ..
        }
    ));
}

/// The 10-19 colocated resets: the runner and its executor slept together
/// for a quarter of an hour, and the first beats after the wake carried
/// facts whose wall-clock stamp was fifteen minutes old. The executor had
/// been awake for seconds of it. A freeze is counted in time somebody
/// could have recomputed, so the sleep is charged to nobody, while
/// placement still refuses the old facts.
#[test]
fn a_suspended_runner_does_not_charge_its_sleep_to_colocated_facts() {
    let (pool, generation, ..) = colocated_link_with_executing_leader(0);
    {
        let mut connections = pool.lock_connections();
        let entry = connections.get_mut(COLOCATED_EXECUTOR_ID).unwrap();
        entry.last_progress_unix_ms = unix_time_ms();
        Arc::make_mut(&mut entry.advertisement).liveness_observed_at_unix_ms =
            Some(unix_time_ms().saturating_sub(900_000));
        entry.link_clock.stage_facts_recomputed(10_000);
    }

    assert_eq!(
        pool.assess_colocated_link(unix_time_ms(), LinkStallBounds::default()),
        LinkRemediation::Healthy
    );
    let frozen = pool
        .managed_telemetry_stall_ms(COLOCATED_EXECUTOR_ID, generation, unix_time_ms())
        .expect("a link with a recompute stamp reports how long it has been frozen");
    assert!((10_000..15_000).contains(&frozen), "{frozen}");
    assert!(
        pool.executor_health(unix_time_ms())
            .remove(0)
            .telemetry_stale,
        "facts measured before a sleep are still old facts"
    );

    // Awake time with no recompute is a freeze, whatever the wall says.
    pool.link_clock(COLOCATED_EXECUTOR_ID, generation)
        .unwrap()
        .stage_facts_recomputed(EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS + 1_000);
    assert!(matches!(
        pool.assess_colocated_link(unix_time_ms(), LinkStallBounds::default()),
        LinkRemediation::Bounce {
            cause: LinkStallCause::TelemetryFrozen { frozen_ms },
            ..
        } if frozen_ms > EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS
    ));
}

/// What restarts the freeze is the executor's stamp moving, read off the
/// beat as ingest applies it. A beat that carries the same stamp again is
/// a beat, not a recompute.
#[test]
fn only_a_beat_whose_recompute_stamp_moved_restarts_the_freeze() {
    let (pool, generation, ..) = colocated_link_with_executing_leader(0);
    facts_last_measured(&pool, EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS + 1_000);
    let beat = |liveness_observed_at_unix_ms: Option<u64>| {
        let mut advertisement = pool
            .lock_connections()
            .get(COLOCATED_EXECUTOR_ID)
            .unwrap()
            .advertisement
            .as_ref()
            .clone();
        advertisement.observed_at_unix_ms = unix_time_ms();
        advertisement.liveness_observed_at_unix_ms = liveness_observed_at_unix_ms;
        assert!(pool.handle_executor_message(
            COLOCATED_EXECUTOR_ID,
            generation,
            ExecutorMessage::Heartbeat {
                advertisement,
                health: ExecutorSubstrateReport::default(),
                last_update: None,
                machine: None,
            },
        ));
    };
    let stamp = pool
        .lock_connections()
        .get(COLOCATED_EXECUTOR_ID)
        .unwrap()
        .advertisement
        .liveness_observed_at_unix_ms;

    beat(stamp);
    assert!(matches!(
        pool.assess_colocated_link(unix_time_ms(), LinkStallBounds::default()),
        LinkRemediation::Bounce {
            cause: LinkStallCause::TelemetryFrozen { .. },
            ..
        }
    ));

    beat(Some(unix_time_ms()));
    assert_eq!(
        pool.assess_colocated_link(unix_time_ms(), LinkStallBounds::default()),
        LinkRemediation::Healthy
    );
}

/// The wedge arrives on every attached machine inside one window, so the
/// cure must not be a fleet-wide recycle. One turn at a time, and a
/// declined turn is simply retaken on a later poll.
#[test]
fn telemetry_bounces_are_taken_one_machine_at_a_time() {
    let pool = Fleet::default();
    let now = unix_time_ms();

    assert!(pool.claim_telemetry_stall_bounce(now));
    assert!(!pool.claim_telemetry_stall_bounce(now));
    assert!(!pool.claim_telemetry_stall_bounce(now + TELEMETRY_STALL_BOUNCE_SPACING_MS - 1));
    assert!(pool.claim_telemetry_stall_bounce(now + TELEMETRY_STALL_BOUNCE_SPACING_MS));
}

/// Ordering the two bounds is what keeps their verdicts from collapsing:
/// telemetry remediation must never fire on a link silence would have
/// already taken, and neither may fire before the health verdict that warns
/// about it.
#[test]
fn stall_bounds_stay_ordered_against_the_health_verdicts_they_follow() {
    let bounds = LinkStallBounds::default();
    assert!(bounds.telemetry_ms > bounds.silence_ms);
    assert!(bounds.telemetry_ms > EXECUTOR_TELEMETRY_STALE_AFTER_MS);
    assert!(bounds.silence_ms > EXECUTOR_PROGRESS_FRESHNESS_MS);
    assert!(
        TELEMETRY_STALL_BOUNCE_SPACING_MS < bounds.telemetry_ms,
        "pacing must not outlast the condition it paces"
    );
}

#[test]
fn deadline_evidence_preserves_fresh_executor_report() {
    let pool = Fleet::default();
    let (tx, _rx) = mpsc::unbounded_channel();
    let generation = pool.attach_executor(tx);
    let now = unix_time_ms();
    let reported = ExecutorSubstrateEvidence::without_queue(
        ExecutorSubstrateState::InitialStorageSweep,
        now.saturating_sub(10),
        now,
    );
    assert!(pool.set_executor_snapshot(
        COLOCATED_EXECUTOR_ID,
        generation,
        FleetSnapshot {
            substrate_state: Some(reported.clone()),
            ..FleetSnapshot::default()
        },
        ExecutorSubstrateReport::default(),
    ));

    assert_eq!(
        pool.executor_deadline_evidence(COLOCATED_EXECUTOR_ID, generation),
        reported
    );
}

/// The layer that hands the executor its per-item budget. An explicit bound
/// must survive here byte-for-byte — including one far above the old
/// ten-minute cap, which is the exact case whose output the socket used to
/// discard — an omitted bound must become the batch ceiling rather than any
/// smaller default, and only the ceiling itself may shorten a request.
/// A run batch's request carries its items' descriptions as its command, and
/// the executor's dependency catalog must never classify that label. Both
/// halves of the crossing are asserted together: the label the queue and the
/// desktop read, and the Cargo item the executor selects from the serialized
/// batch. Classifying the label is how no run batch was ever offered a
/// catalog (cairn/5289).
#[test]
fn a_run_batch_hands_the_catalog_its_cargo_item_and_keeps_its_label_for_display() {
    let resolved = vec![
        (
            "Warm up".to_string(),
            Ok(RunSpec::Shell {
                command: "echo warm".into(),
                timeout: None,
                args: Vec::new(),
            }),
        ),
        (
            "Run exec tests".to_string(),
            Ok(RunSpec::Shell {
                command: "cargo test -p cairn-exec".into(),
                timeout: None,
                args: Vec::new(),
            }),
        ),
    ];
    let (command, command_class) = crate::mcp::handlers::run::build_slot_command_parts(&resolved);
    let mut run_batch = resolved_process_batch(Vec::new(), true, true);
    run_batch.resolved = resolved;
    let batch = serialize_process_batch(
        run_batch,
        &[],
        "runner-context".into(),
        ProcessSandboxMode::Confined,
        None,
    )
    .unwrap();
    let mut request = provenance_request();
    request.command = command;
    request.command_class = command_class;

    assert_eq!(request.command, "Warm up && Run exec tests");
    assert_eq!(
        request.command_class,
        cairn_common::executor_protocol::CellCommandClass::CargoTest
    );
    let launch = cairn_exec::dependency_catalog_launch(&request, Some(&batch))
        .expect("the batch's Cargo item is offered to the catalog");
    assert_eq!(launch.command, "cargo test -p cairn-exec");
    assert_eq!(
        cairn_exec::dependency_catalog_launch(&request, None)
            .map(|launch| launch.command.into_owned()),
        None,
        "the label alone runs no Cargo, so it is nothing to classify"
    );
}

#[test]
fn process_batch_serialization_preserves_millisecond_timeouts_and_flags() {
    let ceiling = cairn_common::run_contract::RUN_BATCH_CEILING_MS;
    let batch = serialize_process_batch(
        resolved_process_batch(
            vec![Some(3_000), None, Some(3_600_000), Some(u32::MAX)],
            true,
            false,
        ),
        &[(
            "CAIRN_WORKTREE_BRANCH".into(),
            "agent/cairn-2929-builder-0".into(),
        )],
        "runner-context".into(),
        ProcessSandboxMode::Confined,
        None,
    )
    .unwrap();

    assert_eq!(batch.items[0].timeout_ms, 3_000);
    assert_eq!(batch.items[1].timeout_ms, u64::from(ceiling));
    assert_eq!(batch.items[2].timeout_ms, 3_600_000);
    assert_eq!(batch.items[3].timeout_ms, u64::from(ceiling));
    assert_eq!(
        batch.items[0].env,
        [(
            "CAIRN_WORKTREE_BRANCH".into(),
            "agent/cairn-2929-builder-0".into()
        )]
    );
    assert_eq!(batch.items[0].execution, ProcessBatchExecution::NativeShell);
    assert!(batch.items[0].program.is_empty());
    assert_eq!(batch.items[0].args, ["true"]);
    assert!(batch.sequential);
    assert!(!batch.stop_on_error);
}

#[tokio::test]
async fn malformed_live_setup_config_prevents_executor_submission() {
    let temp = tempfile::tempdir().unwrap();
    let repository = temp.path().join("repo");
    std::fs::create_dir_all(repository.join(".cairn")).unwrap();
    std::fs::write(
        repository.join(".cairn/config.yaml"),
        "setupCommands: [unterminated",
    )
    .unwrap();
    let orch = Arc::new(test_orchestrator(temp.path()).await);
    orch.db
        .local
        .execute_script(format!(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) \
                 VALUES ('p', 'default', 'Project', 'p', '{}', 1, 1);",
            repository.to_string_lossy()
        ))
        .await
        .unwrap();

    let pool = Fleet::default();
    let (sender, mut executor) = mpsc::unbounded_channel();
    pool.attach_executor(sender);
    let request = CellRequest {
        request_id: "malformed-setup".into(),
        attempt_id: "attempt".into(),
        project_id: "p".into(),
        repository: RepositoryLocator::ColocatedPath {
            project_id: "p".into(),
            repository_id: "p".into(),
            absolute_path: repository.to_string_lossy().into_owned(),
        },
        base_commit: "base".into(),
        command: "touch command-ran".into(),
        command_class: cairn_common::executor_protocol::CellCommandClass::Other,
        placement_work_class: cairn_common::executor_protocol::PlacementWorkClass::AgentSessions,
        owner: None,
        cwd: String::new(),
        env: Vec::new(),
        priority: CellPriority::ReviewCheck,
        wait_horizon_unix_ms: unix_time_ms() + 5_000,
        waiting_since_unix_ms: 0,
        timeout_ms: 1_000,
        mutation_policy: MutationPolicy::PureVerdict,
        requesting_job_id: None,
        affinity_key: None,
        executor: None,
        pinned_executor_id: None,
        placement_mobility: Default::default(),
        verdict_platforms: Vec::new(),
        avoided_executor_ids: Vec::new(),
        project_requirements: Default::default(),
        command_resource_identity: None,
        resource_reservation: Default::default(),
        learned_estimate: None,
        scratch_sync: None,
        home_affinity: None,
        publication: None,
    };

    let outcome = pool.submit_execution(&orch, request, None).await;
    assert!(matches!(
        outcome,
        CellOutcome::Unavailable {
            reason: CellUnavailableReason::Preparation,
            ref diagnostic,
        } if diagnostic.starts_with(PROJECT_CONFIG_REFUSAL)
            && diagnostic.contains("failed to parse project config")
    ));
    // Attach states the runner's drain posture, so an empty channel is not
    // the available evidence. What this guards is that a refused request
    // reached no executor: nothing beyond that opening frame was sent, and
    // nothing ran.
    assert!(matches!(
        executor.try_recv(),
        Ok(ExecutorMessage::DrainModeRequest { enabled: false, .. })
    ));
    assert!(
        executor.try_recv().is_err(),
        "a request refused during preparation must never reach an executor"
    );
    assert!(!repository.join("command-ran").exists());
}

/// A request presented while the supervisor is still attaching waits for
/// readiness instead of failing, and it waits on the horizon it declared
/// rather than on one selection rewrote for it.
#[tokio::test]
async fn a_request_waits_out_an_attaching_supervisor_within_its_horizon() {
    let pool = Fleet::default();
    pool.declare_colocated_substrate(ExecutorSubstrateState::ProtocolAttaching);
    let attaching = pool.clone();
    let (tx, mut rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(40)).await;
        attaching.attach_executor(tx);
        announce_every_room(&attaching);
        attaching.clear_colocated_substrate();
    });
    let request = CellRequest {
        request_id: "r".into(),
        attempt_id: "a".into(),
        project_id: "p".into(),
        repository: RepositoryLocator::ColocatedPath {
            project_id: "p".into(),
            repository_id: "repo".into(),
            absolute_path: "/repo".into(),
        },
        base_commit: "base".into(),
        command: "true".into(),
        command_class: cairn_common::executor_protocol::CellCommandClass::Other,
        placement_work_class: cairn_common::executor_protocol::PlacementWorkClass::AgentSessions,
        owner: None,
        cwd: String::new(),
        env: Vec::new(),
        priority: CellPriority::ReviewCheck,
        // Long enough to outlast the attach this test stages. A horizon
        // shorter than the work is a requester saying it does not want the
        // result, and nothing rewrites it into one that does.
        wait_horizon_unix_ms: unix_time_ms() + 5_000,
        waiting_since_unix_ms: 0,
        timeout_ms: 1_000,
        mutation_policy: MutationPolicy::PureVerdict,
        requesting_job_id: None,
        affinity_key: None,
        executor: None,
        pinned_executor_id: None,
        placement_mobility: Default::default(),
        verdict_platforms: Vec::new(),
        avoided_executor_ids: Vec::new(),
        project_requirements: Default::default(),
        command_resource_identity: None,
        resource_reservation: Default::default(),
        learned_estimate: None,
        scratch_sync: None,
        home_affinity: None,
        publication: None,
    };
    let config = ExecutorConfig {
        project_id: "p".into(),
        project_key: "p".into(),
        default_timeout_seconds: 1,
        setup_commands: Vec::new(),
        populate: Default::default(),
        population_source_root: None,
    };

    let sender = place_through_dispatcher(&pool, &request, None)
        .await
        .unwrap()
        .selected
        .sender;
    sender
        .send(ExecutorMessage::Granted {
            entry_id: "entry".into(),
            config,
            request,
            batch: None,
        })
        .unwrap();
    assert!(matches!(
        rx.recv().await,
        Some(ExecutorMessage::DrainModeRequest { enabled: false, .. })
    ));
    assert!(matches!(
        rx.recv().await,
        Some(ExecutorMessage::Granted { .. })
    ));
}

#[tokio::test]
async fn wedged_submission_times_out_cancels_and_does_not_block_the_next_attempt() {
    let pool = Fleet::default();
    let (sender, mut executor) = mpsc::unbounded_channel();
    let generation = pool.attach_executor(sender);

    let first_key = ("request-1".to_string(), "attempt-1".to_string());
    let first_rx = claim_entry(&pool, &first_key, COLOCATED_EXECUTOR_ID, generation, None);
    // A wedged submission is one whose grant was delivered, so the guard
    // must cancel the work rather than withdraw a claim.
    let first_guard = EntryDropGuard {
        pool: pool.clone(),
        request_id: first_key.0.clone(),
        attempt_id: first_key.1.clone(),
        entry_id: "entry-1".into(),
        granted_to: Some(COLOCATED_EXECUTOR_ID.into()),
        cause: None,
        armed: true,
    };
    let outcome = tokio::time::timeout(Duration::from_millis(120), first_rx)
        .await
        .map(|result| result.unwrap())
        .unwrap_or_else(|_| executor_unavailable("executor request watchdog expired".into()));
    drop(first_guard);
    assert!(matches!(
        outcome,
        CellOutcome::Unavailable {
            reason: CellUnavailableReason::ExecutorUnavailable,
            ..
        }
    ));
    assert!(!pool.queue.holds(&first_key));
    // Attaching states the runner's drain posture before any work is sent,
    // so the cancellation is not the first frame. Skipping what attach says
    // rather than asserting on frame position keeps this test about the
    // watchdog cancelling a wedged submission, and keeps a future attach
    // frame from turning it red again.
    let cancellation = loop {
        match executor.recv().await {
            Some(ExecutorMessage::DrainModeRequest { .. }) => continue,
            other => break other,
        }
    };
    assert!(matches!(
        cancellation,
        Some(ExecutorMessage::Cancel { request_id, attempt_id })
            if request_id == first_key.0 && attempt_id == first_key.1
    ));

    let second_key = ("request-2".to_string(), "attempt-2".to_string());
    let second_rx = claim_entry(&pool, &second_key, COLOCATED_EXECUTOR_ID, generation, None);
    let completed = CellOutcome::Cancelled {
        request_id: second_key.0.clone(),
        attempt_id: second_key.1.clone(),
    };
    pool.handle_executor_message(
        COLOCATED_EXECUTOR_ID,
        generation,
        ExecutorMessage::Result {
            request_id: second_key.0,
            attempt_id: second_key.1,
            outcome: completed.clone(),
        },
    );
    assert_eq!(second_rx.await.unwrap(), completed);
}

#[tokio::test]
async fn dropped_coalesced_leader_publishes_terminal_outcome() {
    let pool = Fleet::default();
    let leader = ("leader".to_string(), "attempt".to_string());
    let key = CoalescingKey::new(result_identity(), leader.clone());
    let (tx, rx) = oneshot::channel();
    {
        let mut registry = pool.in_flight.lock().unwrap();
        registry.open(
            key.clone(),
            PlacementDemand::default(),
            HashMap::from([(
                leader.clone(),
                CoalescedSubscriber {
                    waiter: tx,
                    priority: CellPriority::ReviewCheck,
                    requesting_job_id: None,
                },
            )]),
        );
    }
    pool.coalesced_leaders
        .lock()
        .unwrap()
        .insert(leader.clone());
    pool.cancelled_leaders
        .lock()
        .unwrap()
        .insert(leader.clone());

    drop(CoalescedLeaderCompletionGuard {
        pool: pool.clone(),
        leader: leader.clone(),
        work_class: PlacementWorkClass::ReviewChecks,
        keys: vec![key.clone()],
        runner_context_id: None,
        armed: true,
    });

    // A vanished leader is Cairn's own fault, and the subscriber is told
    // exactly that rather than handed a machine-loss outcome.
    assert_eq!(
        rx.await.unwrap().err(),
        Some(CoalescedWaitError::LeaderVanished)
    );
    assert!(!pool.in_flight.lock().unwrap().by_key.contains_key(&key));
    assert!(!pool.coalesced_leaders.lock().unwrap().contains(&leader));
    assert!(!pool.cancelled_leaders.lock().unwrap().contains(&leader));
    assert!(!pool.preparing_leaders.lock().unwrap().contains_key(&leader));
}

/// A leader that panicked while holding the in-flight registry leaves it
/// poisoned. Its guard drops during that same unwind, and a second panic
/// there would abort the runner; instead its subscribers are still told
/// the leader vanished.
#[tokio::test]
async fn a_leader_that_panics_holding_the_registry_still_releases_its_subscribers() {
    let pool = Fleet::default();
    let leader = ("leader".to_string(), "attempt".to_string());
    let key = CoalescingKey::new(result_identity(), leader.clone());
    let (tx, rx) = oneshot::channel();
    pool.in_flight.lock().unwrap().open(
        key.clone(),
        PlacementDemand::default(),
        HashMap::from([(
            leader.clone(),
            CoalescedSubscriber {
                waiter: tx,
                priority: CellPriority::ReviewCheck,
                requesting_job_id: None,
            },
        )]),
    );
    let guard = CoalescedLeaderCompletionGuard {
        pool: pool.clone(),
        leader: leader.clone(),
        work_class: PlacementWorkClass::ReviewChecks,
        keys: vec![key.clone()],
        runner_context_id: None,
        armed: true,
    };
    let registry = pool.in_flight.clone();
    let unwound = std::thread::spawn(move || {
        let _guard = guard;
        let _held = registry.lock().unwrap();
        panic!("the leader panicked holding the registry");
    })
    .join();

    assert!(unwound.is_err(), "the leader's thread panicked");
    assert!(pool.in_flight.is_poisoned());
    assert_eq!(
        rx.await.unwrap().err(),
        Some(CoalescedWaitError::LeaderVanished)
    );
    assert!(!lock_through_poison(&pool.in_flight)
        .by_key
        .contains_key(&key));
}

/// The two ways a leader ends without an outcome read differently, and
/// both name what the leader was doing and where.
#[test]
fn an_unfinished_leader_says_whether_it_panicked_or_was_dropped() {
    let leader = ("fa11da35".to_string(), "3158a653".to_string());
    let panicked = unfinished_leader_line(
        &leader,
        PlacementWorkClass::ReviewChecks,
        Some("fedora"),
        true,
    );
    let dropped = unfinished_leader_line(&leader, PlacementWorkClass::ReviewChecks, None, false);

    assert!(
        panicked.contains("coalesced reviewChecks leader"),
        "{panicked}"
    );
    assert!(panicked.contains("its task panicked"), "{panicked}");
    assert!(
        panicked.contains("last reported running on fedora"),
        "{panicked}"
    );
    assert!(
        dropped.contains("its task was dropped unfinished"),
        "{dropped}"
    );
    assert!(
        dropped.contains("no machine reported running it"),
        "{dropped}"
    );
}

#[tokio::test]
async fn old_leader_completion_cannot_complete_a_recycled_result_key() {
    let pool = Fleet::default();
    let leader_a = ("leader-a".to_string(), "attempt-a".to_string());
    let key = CoalescingKey::new(result_identity(), leader_a.clone());
    let subscriber_a = ("subscriber-a".to_string(), "attempt-a".to_string());
    let (tx_a, _rx_a) = oneshot::channel();
    {
        let mut registry = pool.in_flight.lock().unwrap();
        registry.open(
            key.clone(),
            PlacementDemand::default(),
            HashMap::from([(
                subscriber_a.clone(),
                CoalescedSubscriber {
                    waiter: tx_a,
                    priority: CellPriority::ReviewCheck,
                    requesting_job_id: None,
                },
            )]),
        );
    }
    pool.coalesced_leaders
        .lock()
        .unwrap()
        .insert(leader_a.clone());
    let guard_a = CoalescedLeaderCompletionGuard {
        pool: pool.clone(),
        leader: leader_a,
        work_class: PlacementWorkClass::ReviewChecks,
        keys: vec![key.clone()],
        runner_context_id: None,
        armed: true,
    };

    pool.detach_coalesced_subscriber(&subscriber_a, None);

    let leader_b = ("leader-b".to_string(), "attempt-b".to_string());
    let subscriber_b = ("subscriber-b".to_string(), "attempt-b".to_string());
    let key_b = CoalescingKey::new(result_identity(), leader_b.clone());
    let (tx_b, mut rx_b) = oneshot::channel();
    {
        let mut registry = pool.in_flight.lock().unwrap();
        registry.open(
            key_b.clone(),
            PlacementDemand::default(),
            HashMap::from([(
                subscriber_b.clone(),
                CoalescedSubscriber {
                    waiter: tx_b,
                    priority: CellPriority::ReviewCheck,
                    requesting_job_id: None,
                },
            )]),
        );
    }
    pool.coalesced_leaders
        .lock()
        .unwrap()
        .insert(leader_b.clone());

    drop(guard_a);

    {
        let registry = pool.in_flight.lock().unwrap();
        assert!(!registry.by_key.contains_key(&key));
        assert!(registry.by_key.contains_key(&key_b));
    }
    assert!(tokio::time::timeout(Duration::from_millis(10), &mut rx_b)
        .await
        .is_err());

    let completed = CellOutcome::Cancelled {
        request_id: leader_b.0.clone(),
        attempt_id: leader_b.1.clone(),
    };
    assert!(pool.complete_coalesced_for_leader(&key_b, completed.clone()));
    assert_eq!(
        rx_b.await.unwrap().unwrap().outcome,
        restamp_outcome(&completed, &subscriber_b)
    );
}

#[tokio::test]
async fn a_subscriber_waits_out_its_leaders_preparation() {
    let pool = Fleet::default();
    let leader = ("leader".to_string(), "attempt".to_string());
    let subscriber = ("subscriber".to_string(), "attempt".to_string());
    let key = CoalescingKey::new(result_identity(), leader.clone());
    let (tx, rx) = oneshot::channel();
    {
        let mut registry = pool.in_flight.lock().unwrap();
        registry.open(
            key.clone(),
            PlacementDemand::default(),
            HashMap::from([(
                subscriber.clone(),
                CoalescedSubscriber {
                    waiter: tx,
                    priority: CellPriority::ReviewCheck,
                    requesting_job_id: None,
                },
            )]),
        );
    }
    let now = unix_time_ms();
    pool.preparing_leaders.lock().unwrap().insert(
        leader,
        LeaderPreparation {
            since_unix_ms: now,
            last_progress_unix_ms: now,
        },
    );
    let completion_pool = pool.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        completion_pool.complete_coalesced_for_leader(
            &key,
            CellOutcome::Cancelled {
                request_id: "leader".into(),
                attempt_id: "attempt".into(),
            },
        );
    });

    let outcome = pool
        .await_coalesced(subscriber, now + 30_000, rx)
        .await
        .expect("a subscriber must outlive its leader's preparation");
    assert!(matches!(outcome.outcome, CellOutcome::Cancelled { .. }));
}

#[tokio::test]
async fn live_command_process_disables_subscriber_acquisition_deadline() {
    let pool = Fleet::default();
    let (sender, mut executor) = mpsc::unbounded_channel();
    let generation = pool.attach_executor(sender);
    let leader = ("leader".to_string(), "attempt".to_string());
    let subscriber = ("subscriber".to_string(), "attempt".to_string());
    let key = CoalescingKey::new(result_identity(), leader.clone());
    let _pending_rx = claim_entry(&pool, &leader, COLOCATED_EXECUTOR_ID, generation, None);
    let now = unix_time_ms();
    assert!(pool.set_executor_snapshot(
        COLOCATED_EXECUTOR_ID,
        generation,
        FleetSnapshot {
            executing_requests: vec![ExecutingCellRequest {
                command_resource_identity: None,
                executor_id: COLOCATED_EXECUTOR_ID.into(),
                cell_id: "slot-1".into(),
                request_id: leader.0.clone(),
                attempt_id: leader.1.clone(),
                owner: None,
                command_class: cairn_common::executor_protocol::CellCommandClass::Other,
                command: "check".into(),
                priority: Some(CellPriority::ReviewCheck),
                subscriber_count: 1,
                resource_reservation: ResourceReservation::default(),
                learned_estimate: None,
                started_at_unix_ms: now,
                process_ids: vec![42],
                live_usage: None,
                build_jobs: None,
                adopted: None,
                memory_phase: None,
                standalone: false,
            }],
            ..FleetSnapshot::default()
        },
        ExecutorSubstrateReport::default(),
    ));
    pool.lock_connections()
        .get_mut(COLOCATED_EXECUTOR_ID)
        .unwrap()
        .last_progress_unix_ms = now.saturating_sub(EXECUTOR_PROGRESS_FRESHNESS_MS + 1);
    assert!(
        pool.request_substrate_hold(
            COLOCATED_EXECUTOR_ID,
            generation,
            &leader.0,
            "different-attempt",
        )
        .is_none(),
        "a recycled request ID must not inherit another attempt's running exemption"
    );
    let (tx, rx) = oneshot::channel();
    {
        let mut registry = pool.in_flight.lock().unwrap();
        registry.open(
            key.clone(),
            PlacementDemand::default(),
            HashMap::from([(
                subscriber.clone(),
                CoalescedSubscriber {
                    waiter: tx,
                    priority: CellPriority::ReviewCheck,
                    requesting_job_id: None,
                },
            )]),
        );
    }
    let completion_pool = pool.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        completion_pool.complete_coalesced_for_leader(
            &key,
            CellOutcome::Cancelled {
                request_id: leader.0.clone(),
                attempt_id: leader.1.clone(),
            },
        );
    });

    let outcome = pool
        .await_coalesced(subscriber, now + 5, rx)
        .await
        .expect("a kernel-live command must outlive the acquisition deadline");
    assert!(matches!(outcome.outcome, CellOutcome::Cancelled { .. }));
    // Attaching states the runner's drain posture, so the channel is not
    // empty to begin with. What this guards is that no *cancellation* went
    // to the machine, which is a different claim from the machine hearing
    // nothing at all.
    assert!(matches!(
        executor.try_recv(),
        Ok(ExecutorMessage::DrainModeRequest { enabled: false, .. })
    ));
    assert!(matches!(
        executor.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
}

/// A cell is not the live checkout, however much it looks like one on disk.
/// The marker test that used to answer this called every cell the live
/// checkout, which turned a fenced cell batch's sandbox denial into a
/// read-only-checkout refusal instead of the fence prompt an `ask` agent's
/// dial asked for.
#[test]
fn a_cell_is_not_the_live_checkout_even_without_a_jj_marker() {
    let cell_on_disk = tempfile::tempdir().unwrap();
    assert!(
        !crate::jj::is_jj_dir(cell_on_disk.path()),
        "a cell checkout carries no jj marker; that is the trap"
    );

    assert!(!runs_in_live_checkout(&RepositoryLocator::ColocatedPath {
        project_id: "p".into(),
        repository_id: "p".into(),
        absolute_path: cell_on_disk.path().to_string_lossy().into_owned(),
    }));
    assert!(!runs_in_live_checkout(&RepositoryLocator::ManagedObjects {
        project_id: "p".into(),
        repository_id: "p".into(),
        object_format: cairn_common::executor_protocol::GitObjectFormat::Sha1,
    }));
    assert!(runs_in_live_checkout(
        &RepositoryLocator::ExistingCheckout {
            project_id: "p".into(),
            repository_id: "p".into(),
            absolute_path: "/live".into(),
        }
    ));
}

/// The dial is the whole gate. An externally owned live checkout used to take
/// a read-only profile structurally, so an `allow` agent's ambient batch was
/// kernel-denied writing anything outside temp and the toolchain caches —
/// `~/.cairn/jj-stores`, a slot directory, the checkout's own target dir
/// (CAIRN-3227). The shape a *fenced* batch gets is unchanged.
#[test]
fn batch_confinement_follows_the_fence_dial_on_every_repository_shape() {
    use crate::models::Fence;
    let live = RepositoryLocator::ExistingCheckout {
        project_id: "p".into(),
        repository_id: "p".into(),
        absolute_path: "/live".into(),
    };
    let cell = RepositoryLocator::ColocatedPath {
        project_id: "p".into(),
        repository_id: "p".into(),
        absolute_path: "/repo".into(),
    };

    for repository in [&live, &cell] {
        assert_eq!(
            Fleet::batch_sandbox_mode(Some(Fence::Allow), repository),
            ProcessSandboxMode::CredentialFloor,
            "allow means no fence profile, on every repository shape, and \
                 never an agent batch that can read the operator credential"
        );
        assert_eq!(
            Fleet::batch_sandbox_mode(None, repository),
            ProcessSandboxMode::CredentialFloor,
            "a batch with no run identity has no fence, but it is still agent work"
        );
    }

    assert_eq!(
        Fleet::batch_sandbox_mode(Some(Fence::Ask), &live),
        ProcessSandboxMode::ReadOnlyCheckout
    );
    assert_eq!(
        Fleet::batch_sandbox_mode(Some(Fence::Deny), &live),
        ProcessSandboxMode::ReadOnlyCheckout
    );
    assert_eq!(
        Fleet::batch_sandbox_mode(Some(Fence::Ask), &cell),
        ProcessSandboxMode::Confined
    );
    assert_eq!(
        Fleet::batch_sandbox_mode(Some(Fence::Deny), &cell),
        ProcessSandboxMode::Confined
    );
}

#[test]
fn check_cadence_batches_run_unconfined() {
    // Both cadences submit project-declared commands sourced from the live
    // main checkout, and docs/checks.md specifies they run with host
    // permissions. Confining them nests a macOS sandbox (exit 71) and sets
    // CAIRN_SANDBOXED, which turns the whole review lane structurally red on
    // every branch — CAIRN-3124. Mutation containment belongs to
    // MutationPolicy, not to this knob.
    let batch = Fleet::check_process_batch(
        Vec::new(),
        Some("ctx".into()),
        None,
        Fleet::WRITE_CHECK_SILENCE_BOUND_MS,
    );
    assert_eq!(
        batch.sandbox_mode,
        ProcessSandboxMode::BranchCode,
        "no fence profile, and the operator credential boundary wherever there is a \
             credential for the branch's code to read"
    );
    assert!(batch.sequential, "checks run in deterministic plan order");
    assert!(
        !batch.stop_on_error,
        "one red check must not hide the verdicts behind it"
    );
}

#[test]
fn check_batches_materialize_content_environment() {
    let item = ProcessBatchItem {
        header: "protocol".into(),
        stream_id: "protocol".into(),
        execution: ProcessBatchExecution::Direct,
        program: "true".into(),
        args: Vec::new(),
        env: vec![
            (
                "CAIRN_CHECK_CHANGED_FILES_CONTENT".into(),
                "src/lib.rs".into(),
            ),
            (
                "CAIRN_CHECK_BASE_CONTENT_CONTENT".into(),
                "{\"src/lib.rs\":\"base\"}".into(),
            ),
            ("PRESERVED".into(), "yes".into()),
        ],
        stdin: None,
        timeout_ms: 1,
        command_resource_identity: None,
        verdict_environment_names: Vec::new(),
        silence_bound_ms: 0,
    };

    let batch =
        Fleet::check_process_batch(vec![item], None, None, Fleet::WRITE_CHECK_SILENCE_BOUND_MS);
    assert_eq!(batch.files.len(), 2);
    assert_eq!(batch.files[0].env, "CAIRN_CHECK_CHANGED_FILES");
    assert_eq!(batch.files[0].contents, "src/lib.rs");
    assert_eq!(batch.files[1].env, "CAIRN_CHECK_BASE_CONTENT");
    assert_eq!(batch.files[1].contents, "{\"src/lib.rs\":\"base\"}");
    assert_eq!(batch.items[0].env, vec![("PRESERVED".into(), "yes".into())]);
}

#[test]
fn check_batches_discard_declared_duration_kill_budgets() {
    let item = |header: &str, timeout_ms| ProcessBatchItem {
        header: header.into(),
        stream_id: header.into(),
        execution: ProcessBatchExecution::Direct,
        program: "true".into(),
        args: Vec::new(),
        env: Vec::new(),
        stdin: None,
        timeout_ms,
        silence_bound_ms: 0,
        command_resource_identity: None,
        verdict_environment_names: Vec::new(),
    };

    let batch = Fleet::check_process_batch(
        vec![item("learned", 180_000), item("unmeasured", 900_000)],
        None,
        None,
        Fleet::REVIEW_CHECK_SILENCE_BOUND_MS,
    );

    assert!(
        batch.items.iter().all(|item| item.timeout_ms == 0),
        "configured and learned duration bounds both lose kill authority"
    );
    assert!(batch.timeout_bases.is_empty());
    // The only authority a check item keeps is the stall bound, and it is
    // the cadence's, whatever duration the item declared or learned.
    assert!(batch
        .items
        .iter()
        .all(|item| item.silence_bound_ms == Fleet::REVIEW_CHECK_SILENCE_BOUND_MS));
}

#[tokio::test]
async fn absent_executor_fails_fast_with_typed_unavailable() {
    let pool = Fleet::default();
    let request = CellRequest {
        request_id: "r".into(),
        attempt_id: "a".into(),
        project_id: "p".into(),
        repository: RepositoryLocator::ColocatedPath {
            project_id: "p".into(),
            repository_id: "repo".into(),
            absolute_path: "/repo".into(),
        },
        base_commit: "base".into(),
        command: "true".into(),
        command_class: cairn_common::executor_protocol::CellCommandClass::Other,
        placement_work_class: cairn_common::executor_protocol::PlacementWorkClass::AgentSessions,
        owner: None,
        cwd: String::new(),
        env: Vec::new(),
        priority: CellPriority::ReviewCheck,
        wait_horizon_unix_ms: unix_time_ms() + 25,
        waiting_since_unix_ms: 0,
        timeout_ms: 1_000,
        mutation_policy: MutationPolicy::PureVerdict,
        requesting_job_id: None,
        affinity_key: None,
        executor: None,
        pinned_executor_id: None,
        placement_mobility: Default::default(),
        verdict_platforms: Vec::new(),
        avoided_executor_ids: Vec::new(),
        project_requirements: Default::default(),
        command_resource_identity: None,
        resource_reservation: Default::default(),
        learned_estimate: None,
        scratch_sync: None,
        home_affinity: None,
        publication: None,
    };
    let started = Instant::now();
    let outcome = place_through_dispatcher(&pool, &request, None)
        .await
        .unwrap_err();
    assert!(started.elapsed() < Duration::from_millis(20));
    assert!(matches!(
        outcome,
        CellOutcome::Unavailable {
            reason: CellUnavailableReason::ExecutorUnavailable,
            ..
        }
    ));
}

#[test]
fn memory_diagnostic_cache_is_generation_fenced_and_expiry_is_read_only() {
    use cairn_common::executor_protocol::MemoryDiagnosticEvidence;
    let pool = Fleet::default();
    let (id, mut entry) = fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]);
    let evidence = MemoryDiagnosticEvidence {
        run_id: "registered".into(),
        anchor: cairn_common::process_identity::ProcessAnchor {
            pid: 42,
            start_token: "1".into(),
        },
        expires_at_unix_ms: NOW + 60_000,
        cached: None,
    };
    Arc::make_mut(&mut entry.health).memory_diagnostic = Some(evidence.clone());
    pool.lock_connections().insert(id, entry);
    assert_eq!(pool.memory_diagnostic(1, NOW), Some(evidence));
    assert!(pool.memory_diagnostic(2, NOW).is_none());
    assert!(pool.memory_diagnostic(1, NOW + 60_000).is_none());
    assert!(
        pool.lock_connections()[COLOCATED_EXECUTOR_ID]
            .health
            .memory_diagnostic
            .is_some(),
        "reads must not clean up or mutate the watch"
    );
}

#[tokio::test]
async fn memory_diagnostic_rpc_is_local_and_correlates_generation() {
    let pool = Fleet::default();
    let (id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]);
    let (sender, mut received) = mpsc::unbounded_channel();
    local.sender = sender;
    pool.lock_connections().insert(id, local);
    let (remote_id, remote) = fleet_entry("remote", "linux", 0, &[]);
    pool.lock_connections().insert(remote_id, remote);

    assert!(pool.set_memory_diagnostic(2, None).await.is_err());
    assert!(received.try_recv().is_err());
    if cfg!(debug_assertions) {
        let request = pool.set_memory_diagnostic(1, None);
        let reply = async {
            let ExecutorMessage::MemoryDiagnosticRequest {
                correlation_id,
                expected_generation,
                attach,
            } = received.recv().await.unwrap()
            else {
                panic!("expected diagnostic control");
            };
            assert_eq!(expected_generation, 1);
            assert!(attach.is_none());
            for (source, generation) in [("remote", 1), (COLOCATED_EXECUTOR_ID, 2)] {
                pool.handle_executor_message(
                    source,
                    generation,
                    ExecutorMessage::MemoryDiagnosticResponse {
                        correlation_id: correlation_id.clone(),
                        result: Ok(()),
                    },
                );
                assert!(pool
                    .pending_memory_diagnostic
                    .lock()
                    .unwrap()
                    .contains_key(&correlation_id));
            }
            pool.handle_executor_message(
                COLOCATED_EXECUTOR_ID,
                1,
                ExecutorMessage::MemoryDiagnosticResponse {
                    correlation_id,
                    result: Ok(()),
                },
            );
        };
        let (result, ()) = tokio::join!(request, reply);
        assert!(result.is_ok());
        assert!(pool.pending_memory_diagnostic.lock().unwrap().is_empty());
    } else {
        assert!(pool.set_memory_diagnostic(1, None).await.is_err());
        assert!(received.try_recv().is_err());
    }

    pool.lock_connections()
        .get_mut(COLOCATED_EXECUTOR_ID)
        .unwrap()
        .colocated = false;
    assert!(pool.set_memory_diagnostic(1, None).await.is_err());
    assert!(received.try_recv().is_err());
}

fn runtime_update_hold_fixture(entry: &mut ExecutorConnectionState, enabled: bool) {
    published_drains_fixture(entry, enabled, false);
}

impl Fleet {
    pub(crate) fn set_update_hold_for_test(&self, executor_id: &str, enabled: bool) {
        runtime_update_hold_fixture(
            self.lock_connections()
                .get_mut(executor_id)
                .expect("attached executor"),
            enabled,
        );
        self.connection_ready.notify_waiters();
    }
}

/// What the published projection leaves on a machine: the self-update
/// rollout's runtime record and, optionally, an operator drain beside it.
fn published_drains_fixture(entry: &mut ExecutorConnectionState, update: bool, operator: bool) {
    use cairn_config::drain::{AdmissionEffect, RuntimeDrainContext, RuntimeDrainKind};
    let fleet = Fleet::default();
    let mut config = FleetConfig::default();
    let principal = cairn_common::identity::PrincipalRef::Human {
        issuer: "test".into(),
        subject: "runtime-fixture".into(),
        organization: None,
    };
    if operator {
        config
            .drains
            .impose_operator(&entry.identity.executor_id, &principal, "maintenance", 1)
            .unwrap();
    }
    if update {
        config
            .drains
            .impose_runtime(
                &entry.identity.executor_id,
                RuntimeDrainContext {
                    kind: RuntimeDrainKind::SelfUpdate,
                    operation_id: "update".into(),
                    turn_id: "turn".into(),
                    runner_boot_id: "boot".into(),
                    admission_effect: AdmissionEffect::RefuseNewEnvironments,
                },
                &principal,
                "executor update",
                2,
            )
            .unwrap();
    }
    fleet.publish_drain_projection(&config);
    let projection = fleet.drain_projection.lock().unwrap();
    entry.drain_state = projection.view(&entry.identity.executor_id);
    entry.drain_blockers = projection.blockers(&entry.identity.executor_id);
    entry.drained = !entry.drain_blockers.is_empty();
    entry.update_hold = update;
}

fn fleet_entry(
    id: &str,
    os: &str,
    load: usize,
    warm: &[&str],
) -> (String, ExecutorConnectionState) {
    fleet_entry_at(id, os, load, warm, NOW)
}

/// What this runner sends a machine about its results: a settled result is
/// acknowledged at once unless a run batch deferred it, and then only when
/// the batch's guard drops; requests an earlier process's rows still claim
/// are named in every liveness report.
#[test]
fn results_are_acknowledged_when_their_consumer_is_done_and_adopted_requests_stay_alive() {
    let fleet = Fleet::default();
    let mut frames = fleet.attach_reported_machine_for_test("exec-a", &[]);
    let acks = |frames: &mut mpsc::UnboundedReceiver<ExecutorMessage>| {
        let mut acked = Vec::new();
        while let Ok(frame) = frames.try_recv() {
            if let ExecutorMessage::ResultAck {
                request_id,
                attempt_id,
            } = frame
            {
                acked.push(format!("{request_id}/{attempt_id}"));
            }
        }
        acked
    };

    fleet.acknowledge_settled_result("exec-a", &("check".into(), "a1".into()));
    assert_eq!(
        acks(&mut frames),
        ["check/a1"],
        "an ordinary result is acknowledged as it settles"
    );

    let guard = fleet.defer_result_ack("batch");
    fleet.acknowledge_settled_result("exec-a", &("batch".into(), "a1".into()));
    fleet.acknowledge_settled_result("exec-a", &("batch".into(), "a2".into()));
    assert!(
        acks(&mut frames).is_empty(),
        "a run batch's result waits for its answer to land"
    );
    drop(guard);
    assert_eq!(acks(&mut frames), ["batch/a1", "batch/a2"]);
    fleet.acknowledge_settled_result("exec-a", &("batch".into(), "a3".into()));
    assert_eq!(
        acks(&mut frames),
        ["batch/a3"],
        "once the batch is done, a late result is acknowledged as it settles"
    );

    fleet.set_adopting(["adopted".to_string()].into_iter().collect());
    assert!(fleet
        .waiting_request_ids("exec-a", 1)
        .contains(&"adopted".to_string()));
}

/// What a batch's durable row names as its machine when placement did not
/// bind it: the machine that claimed its attempt, never one it was only
/// offered to.
#[test]
fn a_request_is_named_by_the_machine_that_claimed_it_never_by_an_offer() {
    let fleet = Fleet::default();
    assert_eq!(fleet.claimed_executor_of("batch"), None);
    let (entry, _waiter) = queue::tests::enqueue(
        &fleet.queue,
        "batch",
        cairn_common::executor_protocol::PlacementMobility::SpillEligible,
    );
    assert_eq!(fleet.claimed_executor_of("batch"), None);
    assert!(fleet.queue.offer(&entry, "exec-b", "exec-b", 1, 2));
    assert_eq!(
        fleet.claimed_executor_of("batch"),
        None,
        "an offer is not a claim"
    );
    assert_eq!(
        fleet.queue.claim(&entry, "exec-b", 1, 3),
        queue::ClaimVerdict::Granted
    );
    assert_eq!(
        fleet.claimed_executor_of("batch").as_deref(),
        Some("exec-b")
    );
}

impl Fleet {
    /// A live colocated job home and its executor link, for tests that need to
    /// observe real residency operations without launching an executor process.
    pub(crate) fn attach_job_home_for_test(
        &self,
        project_id: &str,
        job_id: &str,
        repository_path: &Path,
    ) -> (
        mpsc::UnboundedReceiver<ExecutorMessage>,
        u64,
        cairn_common::executor_protocol::PersistentCellState,
    ) {
        let (id, mut connection) = fleet_entry_at(
            COLOCATED_EXECUTOR_ID,
            std::env::consts::OS,
            0,
            &[],
            unix_time_ms(),
        );
        let (sender, receiver) = mpsc::unbounded_channel();
        connection.sender = sender;
        connection.snapshots_received = 1;
        let mut cell = materialization_read_cell("terminal-refresh-home", job_id);
        cell.project_id = project_id.into();
        cell.executor_id = id.clone();
        let residency = cell.residency.as_mut().unwrap();
        residency.repository = RepositoryLocator::ColocatedPath {
            project_id: project_id.into(),
            repository_id: project_id.into(),
            absolute_path: repository_path.to_string_lossy().into_owned(),
        };
        residency.owner_ref.as_mut().unwrap().project_id = project_id.into();
        let route = ResidencyRoute {
            holder: residency.holder.clone(),
            repository: residency.repository.clone(),
            executor_id: id.clone(),
            pending: false,
        };
        Arc::make_mut(&mut connection.snapshot).cells = vec![cell.clone()];
        let generation = connection.generation;
        self.lock_connections().insert(id.clone(), connection);
        self.residency_routes
            .lock()
            .unwrap()
            .insert((id, route.holder.storage_key()), route);
        (receiver, generation, cell)
    }

    /// Attach one test executor whose snapshot reports exactly `cells`, so a
    /// caller outside this module can exercise a decision that reads
    /// [`Fleet::snapshot`].
    pub(crate) fn seed_cells_for_test(
        &self,
        cells: Vec<cairn_common::executor_protocol::PersistentCellState>,
    ) {
        let (id, mut connection) = fleet_entry("executor-test", "linux", 0, &[]);
        Arc::make_mut(&mut connection.snapshot).cells = cells;
        self.lock_connections().insert(id, connection);
    }

    /// Attach one machine that has reported its account once, listing
    /// `granted` as attempts it still holds, and hand back every frame this
    /// runner sends it.
    pub(crate) fn attach_unreported_machine_for_test(
        &self,
        id: &str,
    ) -> mpsc::UnboundedReceiver<ExecutorMessage> {
        let (id, mut connection) = fleet_entry(id, "linux", 0, &[]);
        let (sender, receiver) = mpsc::unbounded_channel();
        connection.sender = sender;
        connection.snapshots_received = 0;
        self.lock_connections().insert(id, connection);
        receiver
    }

    pub(crate) fn attach_reported_machine_for_test(
        &self,
        id: &str,
        granted: &[(&str, &str)],
    ) -> mpsc::UnboundedReceiver<ExecutorMessage> {
        let (id, mut connection) = fleet_entry_at(id, "linux", 0, &[], unix_time_ms());
        let (sender, receiver) = mpsc::unbounded_channel();
        connection.sender = sender;
        connection.snapshots_received = 1;
        Arc::make_mut(&mut connection.snapshot).granted_attempts = granted
            .iter()
            .map(
                |(request_id, attempt_id)| cairn_common::executor_protocol::GrantedAttempt {
                    request_id: (*request_id).into(),
                    attempt_id: (*attempt_id).into(),
                },
            )
            .collect();
        let generation = connection.generation;
        self.lock_connections().insert(id.clone(), connection);
        // A real executor announces its room as it attaches (cairn/7303).
        self.announce_room_for_test(&id, generation);
        receiver
    }

    /// Announce, for `id` on `generation`, a room that fits any test's work:
    /// what a real executor sends as it attaches, and what the dispatcher
    /// places by.
    pub(crate) fn announce_room_for_test(&self, id: &str, generation: u64) {
        self.announce_room_with_memory_for_test(id, generation, None);
    }

    /// [`Fleet::attach_executor`], announcing the room a real executor sends
    /// as its link opens: a fake that answers offers itself only receives
    /// one once the dispatcher has a room to place by.
    pub(crate) fn attach_announced_executor(
        &self,
        sender: mpsc::UnboundedSender<ExecutorMessage>,
    ) -> u64 {
        let generation = self.attach_executor(sender);
        self.announce_room_for_test(COLOCATED_EXECUTOR_ID, generation);
        generation
    }

    /// [`Self::announce_room_for_test`], with the memory room `memory`
    /// (available, total) where it is given, as a machine whose posted
    /// reading is that would announce.
    pub(crate) fn announce_room_with_memory_for_test(
        &self,
        id: &str,
        generation: u64,
        memory: Option<(u64, u64)>,
    ) {
        use cairn_common::executor_protocol::{
            Allowance, AnnouncedRoom, CompileSlots, DiskRoom, MemoryRoom,
        };
        const GIB: u64 = 1 << 30;
        let (available_bytes, total_bytes) = memory.unwrap_or((512 * GIB, 512 * GIB));
        // Newer than whatever the link announced before, as a real
        // executor's sequence is; an older or equal one changes nothing.
        let seq = self
            .lock_connections()
            .get(id)
            .and_then(|entry| entry.room.as_ref().map(|held| held.seq))
            .unwrap_or(0)
            + 1;
        self.record_room(
            id,
            generation,
            seq,
            AnnouncedRoom {
                slots: CompileSlots::of(64),
                compiling: 0,
                test_cores: 0,
                standing: Default::default(),
                cores: Allowance {
                    limit: 64,
                    committed: 0,
                },
                memory: Some(MemoryRoom {
                    available_bytes,
                    owed_bytes: 0,
                    total_bytes,
                }),
                memory_budget: None,
                disk: Some(DiskRoom {
                    free_bytes: 10_000 * GIB,
                    watermark_bytes: GIB,
                    reserved_bytes: 0,
                    inodes: None,
                }),
                disk_budget: None,
                reclaimable_memory_bytes: 0,
                alone: None,
                provisioning: Vec::new(),
                holds: Vec::new(),
                closures: Vec::new(),
            },
        );
    }

    /// A placement template a test can persist a settlement with.
    pub(crate) fn cell_request_for_test(&self, request_id: &str) -> CellRequest {
        let mut request = targeted_request("linux");
        request.request_id = request_id.into();
        request
    }

    /// Execution metadata for a test's completed outcome.
    pub(crate) fn execution_meta_for_test(
        &self,
    ) -> cairn_common::executor_protocol::CellExecutionMeta {
        cairn_common::executor_protocol::CellExecutionMeta {
            stop: None,
            ..resource_profiles::oom_killed_for_test(0, None, 0, 1)
        }
    }

    /// Attach one test executor named `name` whose posted health reports
    /// exactly `host`.
    pub(crate) fn seed_host_for_test(
        &self,
        name: &str,
        host: cairn_common::executor_protocol::HostHealth,
    ) {
        let (id, mut connection) = fleet_entry(name, "linux", 0, &[]);
        Arc::make_mut(&mut connection.health).host = host;
        self.lock_connections().insert(id, connection);
    }
}

pub(super) fn fleet_entry_at(
    id: &str,
    os: &str,
    load: usize,
    warm: &[&str],
    observed_at_unix_ms: u64,
) -> (String, ExecutorConnectionState) {
    let (sender, receiver) = mpsc::unbounded_channel();
    std::mem::forget(receiver);
    let identity = ExecutorIdentity {
        device_id: format!("device-{id}"),
        executor_id: id.into(),
        display_name: id.into(),
    };
    (
        id.into(),
        ExecutorConnectionState {
            drained: false,
            drain_blockers: Vec::new(),
            drain_state: Default::default(),
            update_hold: false,
            machine_role: MachineRole::General,
            model_residents: Vec::new(),
            identity: identity.clone(),
            advertisement: Arc::new(ExecutorAdvertisement {
                identity,
                capabilities: ExecutorCapabilities {
                    os: os.into(),
                    arch: "x86_64".into(),
                    logical_cores: 8,
                    concurrency_capacity: None,
                    toolchains: vec!["rust".into()],
                    projects_served: vec!["p".into()],
                    disk_budget_bytes: None,
                    memory_budget_bytes: None,
                    sandbox: None,
                    toolchain_detection: None,
                    build_prerequisites: None,
                    containment: None,
                    page_call_programs: None,
                    github_credential: None,
                    publish_isolation: None,
                    machine_services: Vec::new(),
                    credential_floor_service_residents: false,
                    resident_output_end: false,
                    service_bounds: None,
                },
                current_load: load,
                observed_at_unix_ms,
                liveness_observed_at_unix_ms: None,
                facts_stale_refresh_blocked: None,
                last_refresh: None,
                node_scratches: Vec::new(),
            }),
            warm_roots: Arc::new(warm_roots::WarmRootSet::from_inventory(WarmRootInventory {
                revision: 1,
                repositories: vec![test_warm_roots("repo", warm)],
            })),
            warm_roots_resync_requested: false,
            generation: 1,
            sender,
            snapshot: Arc::default(),
            snapshots_received: 0,
            witnessed_grants: HashSet::new(),
            last_progress_unix_ms: observed_at_unix_ms,
            health: Arc::new(ExecutorSubstrateReport {
                applied_policy: cairn_common::executor_protocol::ExecutorRuntimePolicy {
                    maximum_queue_depth: 64,
                    ..Default::default()
                },
                ..Default::default()
            }),
            publishes_machine_samples: false,
            live_usage: load::LiveUsageIndex::default(),
            executor_build_id: None,
            last_update: None,
            colocated: id == COLOCATED_EXECUTOR_ID,
            preparation_refusals: HashMap::new(),
            link_clock: Arc::default(),
            link_path: None,
            room: None,
        },
    )
}

/// A machine with complete, fresh placement readings.
fn measured(
    entry: &mut ExecutorConnectionState,
    cpu_utilization: f64,
    available_memory_bytes: u64,
    free_volume_bytes: u64,
) {
    measured_at(
        entry,
        cpu_utilization,
        available_memory_bytes,
        free_volume_bytes,
        NOW,
    );
}

pub(super) fn measured_at(
    entry: &mut ExecutorConnectionState,
    cpu_utilization: f64,
    available_memory_bytes: u64,
    free_volume_bytes: u64,
    measured_at_unix_ms: u64,
) {
    Arc::make_mut(&mut entry.health).machine = cairn_common::executor_protocol::MachineTelemetry {
        host_temp: None,
        cpu: Measurement::measured(
            measured_at_unix_ms,
            cairn_common::executor_protocol::CpuPressure {
                utilization: cpu_utilization,
                user: cpu_utilization,
                system: 0.0,
                logical_cores: 8,
                clusters: None,
            },
        ),
        memory: Measurement::measured(
            measured_at_unix_ms,
            cairn_common::executor_protocol::MachineMemory {
                total_bytes: 64 * 1024 * 1024 * 1024,
                available_bytes: available_memory_bytes,
            },
        ),
        volume: Measurement::measured(
            measured_at_unix_ms,
            cairn_common::executor_protocol::MachineVolume {
                total_inodes: None,
                free_inodes: None,
                total_bytes: 1024 * 1024 * 1024 * 1024,
                free_bytes: free_volume_bytes,
            },
        ),
        ..Default::default()
    };
}

pub(super) fn spillable_request() -> CellRequest {
    let mut request = targeted_request("linux");
    request.executor = None;
    request.placement_mobility = PlacementMobility::SpillEligible;
    request
}

/// An enrolled machine answers to the name this runner enrolled it under,
/// not the label its own executor.json carries, and a rename re-declaring it
/// moves the live connection's name with no redial (cairn/6637).
#[test]
fn an_enrolled_machine_is_selected_by_its_enrolled_name_and_follows_a_rename() {
    let fleet = Fleet::default();
    fleet.declare_enrolled_remote("fedora", "bginf-halo", "linux", "x86_64");
    let (_, entry) = fleet_entry("fedora", "linux", 0, &[]);
    assert_eq!(entry.advertisement.identity.display_name, "fedora");
    fleet
        .attach_advertised_executor(
            Arc::unwrap_or_clone(entry.advertisement),
            Default::default(),
            entry.sender,
            false,
            None,
            None,
        )
        .unwrap();
    let selects = |name: &str| {
        let connections = fleet.lock_connections();
        matches_selector(
            &connections["fedora"],
            &ExecutorSelector {
                name: Some(name.into()),
                ..ExecutorSelector::default()
            },
        )
    };

    assert_eq!(
        fleet.executor_public_name("fedora").as_deref(),
        Some("bginf-halo")
    );
    assert!(selects("bginf-halo"));
    assert!(!selects("fedora"), "one machine, one address");
    let refusal = own_label_refusal(&fleet.lock_connections(), "fedora")
        .expect("a selector naming the machine's own label is told both names");
    assert!(
        refusal.contains("this runner has it enrolled as bginf-halo"),
        "{refusal}"
    );
    assert_eq!(
        own_label_refusal(&fleet.lock_connections(), "bginf-halo"),
        None
    );
    assert_eq!(
        fleet.lock_connections()["fedora"]
            .advertisement
            .identity
            .display_name,
        "fedora",
        "the advertisement keeps the machine's own words"
    );

    fleet.declare_enrolled_remote("fedora", "fedora", "linux", "x86_64");
    assert_eq!(
        fleet.executor_public_name("fedora").as_deref(),
        Some("fedora")
    );
    assert!(selects("fedora"));
    assert!(!selects("bginf-halo"));

    // A machine with no enrollment record keeps its own label.
    let (_, stray) = fleet_entry("dialed-in", "linux", 0, &[]);
    fleet
        .attach_advertised_executor(
            Arc::unwrap_or_clone(stray.advertisement),
            Default::default(),
            stray.sender,
            false,
            None,
            None,
        )
        .unwrap();
    assert_eq!(
        fleet.executor_public_name("dialed-in").as_deref(),
        Some("dialed-in")
    );
}

/// cairn/5360's acceptance at placement: a job's environment is on
/// `bginf-halo` and a batch names `fedora`. Unpinned (the run path detaches
/// it), only fedora can take it. Pinned to the environment (the one case a
/// selector-bearing batch stays home: a committing batch beside a live
/// process whose machine could not answer the selector), no machine can,
/// and the environment's machine refuses it for the selector rather than
/// running it there.
#[test]
fn a_batch_naming_another_machine_than_its_environment_runs_there_or_nowhere() {
    let now = unix_time_ms();
    let connections: HashMap<String, ExecutorConnectionState> = [
        fleet_entry_at("bginf-halo", "linux", 0, &[], now),
        fleet_entry_at("fedora", "linux", 0, &[], now),
    ]
    .into_iter()
    .collect();
    let mut request = targeted_request("linux");
    request.repository = RepositoryLocator::ManagedObjects {
        project_id: "p".into(),
        repository_id: "repo".into(),
        object_format: GitObjectFormat::Sha1,
    };
    request.placement_mobility = PlacementMobility::SpillEligible;
    request.executor = Some(ExecutorSelector {
        name: Some("fedora".into()),
        ..ExecutorSelector::default()
    });
    let usable = |request: &CellRequest| -> Vec<String> {
        let mut usable: Vec<String> = connections
            .values()
            .filter(|entry| candidate_rejection(entry, PlacementScope::of(request), now).is_none())
            .map(|entry| entry.identity.executor_id.clone())
            .collect();
        usable.sort();
        usable
    };

    assert_eq!(usable(&request), vec!["fedora".to_string()]);
    assert!(matches!(
        candidate_rejection(
            &connections["bginf-halo"],
            PlacementScope::of(&request),
            now
        ),
        Some(PlacementRejectionReason::SelectorMismatch { .. })
    ));

    request.pinned_executor_id = Some("bginf-halo".into());
    assert!(
        usable(&request).is_empty(),
        "a pin never overrides the selector"
    );
    assert!(matches!(
        candidate_rejection(
            &connections["bginf-halo"],
            PlacementScope::of(&request),
            now
        ),
        Some(PlacementRejectionReason::SelectorMismatch { .. })
    ));
}

/// cairn/7220, the execution thread's ruling: a fresh-pipe floor latched
/// on the home of work that cannot leave it is waited out, visibly, and
/// the home posting recovery is what places the parked entry there.
#[tokio::test]
async fn immobile_work_waits_out_its_homes_pipe_floor_and_places_on_recovery() {
    let pool = Fleet::default();
    let now = unix_time_ms();
    let degraded_at = now - 10_000;
    let (local_id, mut local) = fleet_entry_at(COLOCATED_EXECUTOR_ID, "macos", 0, &[], now);
    measured_at(&mut local, 0.01, 8_000_000_000, 8_000_000_000, now);
    pipes_reading(&mut local, degraded_at, 512);
    let mut connections = HashMap::from([(local_id, local)]);
    for id in ["bbs", "fedora"] {
        let (id, mut remote) = fleet_entry_at(id, "linux", 0, &[], now);
        measured_at(&mut remote, 0.01, 8_000_000_000, 8_000_000_000, now);
        connections.insert(id, remote);
    }
    *pool.lock_connections() = connections;
    announce_every_room(&pool);
    let mut request = spillable_request();
    request.placement_mobility = PlacementMobility::PinnedOrColocated;
    request.verdict_platforms = vec!["macos".into()];
    request.repository = RepositoryLocator::ColocatedPath {
        project_id: "p".into(),
        repository_id: "repo".into(),
        absolute_path: "/repo".into(),
    };
    request.wait_horizon_unix_ms = now + 60_000;
    let (entry, _waiter) = pool
        .queue
        .enqueue(
            queue::NewEntry {
                request: request.clone(),
                admission_kind: CellAdmissionKind::Command,
                interactive: false,
                execution_residency: None,
                standing: false,
            },
            now,
        )
        .unwrap();
    let placing = tokio::spawn({
        let pool = pool.clone();
        let request = request.clone();
        let entry = entry.clone();
        async move {
            assign(
                &pool,
                &entry,
                &request,
                &ActivePlacementPolicy::default_profile(),
                None,
            )
            .await
        }
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        !placing.is_finished(),
        "a latched home is waited out, not refused"
    );

    // The home posts two spaced healthy readings: the floor lifts, and
    // the parked entry is placed there.
    {
        let mut connections = pool.lock_connections();
        let local = connections.get_mut(COLOCATED_EXECUTOR_ID).unwrap();
        pipes_reading(local, degraded_at + 5_000, 65_536);
        pipes_reading(local, unix_time_ms(), 65_536);
    }
    pool.machine_facts_posted(COLOCATED_EXECUTOR_ID);
    let placed = tokio::time::timeout(Duration::from_secs(5), placing)
        .await
        .expect("recovery places the parked entry promptly")
        .unwrap()
        .unwrap_or_else(|refusal| panic!("refused after recovery: {refusal:?}"));
    assert_eq!(placed.selected.executor_id, COLOCATED_EXECUTOR_ID);
}

/// cairn/7220: the parked write lanes' refusals quoted bbs, the first
/// untrusted remote by id, as the machine they could not leave. Work that
/// cannot move is answered by its home alone: a home that cannot prepare
/// the project says so, and a declared trust set its home is outside says
/// so, each naming the home.
#[test]
fn an_immobile_refusal_names_its_home_and_never_another_machine() {
    let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "macos", 0, &[]);
    measured(&mut local, 0.01, 48 * GIB, 900 * GIB);
    advertising(&mut local, &[]);
    let mut connections = HashMap::from([(local_id, local)]);
    for id in ["bbs", "fedora"] {
        let (id, mut remote) = fleet_entry(id, "linux", 0, &[]);
        measured(&mut remote, 0.01, 48 * GIB, 900 * GIB);
        connections.insert(id, remote);
    }
    let mut request = spillable_request();
    request.placement_mobility = PlacementMobility::PinnedOrColocated;
    request.verdict_platforms = vec!["macos".into()];
    request.project_requirements = project_needs(&["bun"], &[]).into();

    let refusal = place(&connections, &request).expect_err("the home cannot prepare it");
    assert!(refusal.contains("cannot leave local"), "{refusal}");
    assert!(refusal.contains("requires bun"), "{refusal}");
    assert!(!refusal.contains("bbs"), "{refusal}");

    *request.project_requirements = Default::default();
    request.verdict_platforms = vec!["linux".into()];
    let refusal = place(&connections, &request).expect_err("the home is not gated on");
    assert!(refusal.contains("cannot leave local"), "{refusal}");
    assert!(refusal.contains("counts only from linux"), "{refusal}");
}

/// Two Linux machines and a suite whose largest process is 10 GiB:
/// bgsrv-ub (13 GiB total, 2 GiB standing) and `other` with `other_total`.
async fn process_fit_fleet(
    db_name: &str,
    other_total: u64,
    other_drained: bool,
) -> (Fleet, Arc<cairn_db::storage::LocalDb>, CellRequest) {
    const GIB: u64 = 1024 * 1024 * 1024;
    let db = Arc::new(crate::storage::migrated_test_db(db_name).await);
    let identity = CommandResourceIdentity {
        version: 1,
        key: "rust-tests:never-seen".into(),
        suite: Some("rust-tests".into()),
    };
    let pool = Fleet::default();
    let now = unix_time_ms();
    let mut connections = HashMap::new();
    let mut measured_on = None;
    for (id, total) in [("bgsrv-ub", 13_000_000_000_u64), ("other", other_total)] {
        let (id, mut entry) = fleet_entry_at(id, "linux", 0, &[], now);
        Arc::make_mut(&mut entry.advertisement)
            .capabilities
            .memory_budget_bytes = Some(total / 100 * 85);
        measured_at(&mut entry, 0.01, total - 3 * GIB, 500 * GIB, now);
        Arc::make_mut(&mut entry.health).machine.memory = Measurement::measured(
            now,
            MachineMemory {
                total_bytes: total,
                available_bytes: total - 3 * GIB,
            },
        );
        Arc::make_mut(&mut entry.health)
            .memory_record
            .standing_bytes = Some(2 * GIB);
        entry.drained = id == "other" && other_drained;
        measured_on.get_or_insert_with(|| {
            ReservationPlan::context_for(
                &entry.identity.device_id,
                &entry.identity.executor_id,
                &entry.advertisement.capabilities,
            )
        });
        connections.insert(id, entry);
    }
    *pool.lock_connections() = connections;
    let mut request = spillable_request();
    request.command_class = cairn_common::executor_protocol::CellCommandClass::CargoTest;
    let suite =
        resource_profiles::suite_key(&request.project_id, Some(&identity), request.command_class);
    resource_profiles::observe_suite(
        db.clone(),
        suite.as_deref(),
        CellCommandClass::CargoTest,
        &measured_on.unwrap(),
        &CellExecutionMeta {
            stop: None,
            peak_process: Some(cairn_common::executor_protocol::PeakProcess {
                bytes: 10 * GIB,
                name: Some("rustc".into()),
            }),
            ..resource_profiles::oom_killed_for_test(GIB, None, 0, 1)
        },
        resource_profiles::CompletionKind::Succeeded,
        resource_profiles::MachineAdmission::default(),
        resource_profiles::CompletedSource::default(),
    )
    .await;
    request.command_resource_identity = Some(identity);
    (pool, db, request)
}

/// cairn/5944's specimen across a runner restart and the terminal's own
/// end. A terminal was started on bgsrv-ub with a `cargo test` compile by
/// a runner that has since been rebuilt; its rustc was killed twice in one
/// census (one millisecond, two pids), the command exited, and the terminal
/// was released, so the machine posts no occupancy for it, only the kills,
/// each carrying the project and command class the terminal started with.
/// Starting another terminal of that class through `StartProcess` learns
/// both kills once, even when replayed after another restart. Their load
/// is unknown, so the kills alone never refuse the next terminal. A later
/// oversized victim does refuse by size; an interactive shell is not judged.
#[tokio::test]
async fn a_terminal_running_across_a_runner_restart_teaches_its_kills_and_refuses_the_next() {
    const GIB: u64 = 1024 * 1024 * 1024;
    let temp = tempfile::tempdir().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    orch.db.local.execute(
        "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p', 'default', 'Project', 'p', ?1, 1, 1)",
        [temp.path().to_string_lossy().into_owned()],
    ).await.unwrap();
    let command = "bun run test:rust -p cairn-core";
    let spec = |launch: Option<&str>| -> cairn_common::executor_protocol::ResidentProcessSpec {
        serde_json::from_value(serde_json::json!({
            "program": "/bin/sh",
            "launchCommand": launch,
        }))
        .unwrap()
    };
    let (id, mut entry) = answering_fleet_entry("bgsrv-ub", "linux", 0, &[]);
    Arc::make_mut(&mut entry.advertisement)
        .capabilities
        .memory_budget_bytes = Some(11_042_782_208);
    {
        let health = Arc::make_mut(&mut entry.health);
        health.machine.memory = Measurement::measured(
            unix_time_ms(),
            MachineMemory {
                total_bytes: 13_000_000_000,
                available_bytes: 9 * GIB,
            },
        );
        health.memory_record.standing_bytes = Some(GIB);
        let killed_at = unix_time_ms();
        health.memory_record.oom_kills = [11, 12]
            .into_iter()
            .map(|pid| cairn_common::executor_protocol::OomKillReport {
                killed_at_unix_ms: killed_at,
                pid,
                process_name: Some("rustc".into()),
                footprint_bytes: Some(3 * GIB),
                owner: Some(
                    cairn_common::executor_protocol::LiveUsageSubject::Resident {
                        cell_id: "released-home".into(),
                        process_key: "terminal:tests".into(),
                        generation: 1,
                    },
                ),
                project_id: Some("p".into()),
                command_class: Some(cairn_common::executor_protocol::CellCommandClass::classify(
                    command,
                )),
            })
            .collect();
    }
    let (sender, mut executor) = mpsc::unbounded_channel();
    entry.sender = sender;
    let generation = entry.generation;
    let selected = selected_executor(&entry);
    let context = ReservationPlan::profile_context(&selected);
    // A fresh environment: nothing of the killed terminal is posted.
    let mut cell = materialization_read_cell("ub-home", "builder");
    cell.executor_id = "bgsrv-ub".into();
    let residency = cell.residency.clone().unwrap();
    Arc::make_mut(&mut entry.snapshot).cells = vec![cell.clone()];
    orch.fleet.lock_connections().insert(id, entry);
    orch.fleet.residency_routes.lock().unwrap().insert(
        ("bgsrv-ub".into(), residency.holder.storage_key()),
        ResidencyRoute {
            holder: residency.holder.clone(),
            repository: residency.repository.clone(),
            executor_id: "bgsrv-ub".into(),
            pending: false,
        },
    );
    let start = |key: &str, launch: Option<&str>| ResidencyOperation::StartProcess {
        fence: ResidencyFence {
            holder: residency.holder.clone(),
            incarnation_id: residency.incarnation_id.clone(),
            cell_epoch: cell.cell_epoch,
        },
        process_key: key.into(),
        kind: cairn_common::executor_protocol::ResidentProcessKind::Terminal { slug: key.into() },
        reservation: None,
        process: spec(launch),
    };
    let fleet = orch.fleet.clone();
    let answered = cell.clone();
    let machine = tokio::spawn(async move {
        for expected in ["terminal:again", "terminal:third", "terminal:shell"] {
            let Some(ExecutorMessage::ResidencyRequest {
                correlation_id,
                operation: ResidencyOperation::StartProcess { process_key, .. },
                ..
            }) = next_work_frame(&mut executor).await
            else {
                panic!("a fitting terminal reaches the executor");
            };
            assert_eq!(process_key, expected);
            fleet.handle_executor_message(
                "bgsrv-ub",
                generation,
                ExecutorMessage::ResidencyResponse {
                    correlation_id,
                    result: ResidencyResult::State {
                        cell: answered.clone(),
                    },
                },
            );
        }
    });
    for key in ["terminal:again", "terminal:third"] {
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            orch.fleet
                .operate_residency(&orch, start(key, Some(command))),
        )
        .await
        .expect("the start settles");
        assert!(
            matches!(result, ResidencyResult::State { .. }),
            "{result:?}"
        );
    }
    let suite = resource_profiles::suite_key(
        "p",
        None,
        cairn_common::executor_protocol::CellCommandClass::classify(command),
    )
    .unwrap();
    let evidence = resource_profiles::resolve_process_demand(
        orch.db.local.clone(),
        None,
        Some(&suite),
        cairn_common::executor_protocol::CellCommandClass::classify(command),
        &context,
    )
    .await;
    let demand = evidence.demand.expect("the killed rustc is learned");
    assert_eq!(demand.peak_bytes, 3 * GIB);
    assert_eq!(demand.name.as_deref(), Some("rustc"));
    let kills = demand.kills_here.unwrap();
    assert_eq!(kills.alone, 0);
    let crowded = kills.crowded.unwrap();
    assert_eq!(crowded.count, 2);
    assert_eq!(crowded.most_co_resident, None);
    let counted = orch
        .db
        .local
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT SUM(oom_crowded_kill_count) FROM suite_memory_evidence",
                        (),
                    )
                    .await?;
                Ok(rows
                    .next()
                    .await?
                    .map(|row| row.get::<i64>(0))
                    .transpose()?)
            })
        })
        .await
        .unwrap();
    assert_eq!(
        counted,
        Some(2),
        "two kills in one millisecond are two kills, once"
    );
    {
        let mut connections = orch.fleet.lock_connections();
        let health = Arc::make_mut(&mut connections.get_mut("bgsrv-ub").unwrap().health);
        let mut oversized = health.memory_record.oom_kills[0].clone();
        oversized.pid = 13;
        oversized.footprint_bytes = Some(12 * GIB);
        health.memory_record.oom_kills.push(oversized);
    }
    let refused = format!(
        "{:?}",
        orch.fleet
            .operate_residency(&orch, start("terminal:oversized", Some(command)))
            .await
    );
    assert!(
        refused.contains("cannot run this terminal's command"),
        "{refused}"
    );
    assert!(
        !refused.contains("while it ran alone"),
        "size, not unknown load, refuses: {refused}"
    );
    let shell = tokio::time::timeout(
        Duration::from_secs(10),
        orch.fleet
            .operate_residency(&orch, start("terminal:shell", None)),
    )
    .await
    .expect("the shell start settles");
    assert!(
        matches!(shell, ResidencyResult::State { .. }),
        "an interactive shell is not judged: {shell:?}"
    );
    machine.await.unwrap();
}

/// The executor keeps its last kills by count, so a kill learned days ago
/// is still posted when a new one arrives. Suite A was killed once three
/// days ago and learned then; suite B is killed now. Learning B prunes old
/// identities, and replaying the ring again and again never counts A's
/// kill a second time: it is past the learning horizon, so A stays at one
/// kill and still fits.
#[tokio::test]
async fn an_old_kill_still_posted_is_never_counted_again() {
    const GIB: u64 = 1024 * 1024 * 1024;
    const DAY: u64 = 24 * 60 * 60 * 1_000;
    let db = Arc::new(crate::storage::migrated_test_db("resident-kill-horizon.db").await);
    let pool = Fleet::default();
    let now = unix_time_ms();
    let (id, mut entry) = answering_fleet_entry("bgsrv-ub", "linux", 0, &[]);
    Arc::make_mut(&mut entry.advertisement)
        .capabilities
        .memory_budget_bytes = Some(11_042_782_208);
    let kill = |pid: u32, at: u64, command: &str| cairn_common::executor_protocol::OomKillReport {
        killed_at_unix_ms: at,
        pid,
        process_name: Some("rustc".into()),
        footprint_bytes: Some(2 * GIB),
        owner: Some(
            cairn_common::executor_protocol::LiveUsageSubject::Resident {
                cell_id: "cell".into(),
                process_key: format!("terminal:{pid}"),
                generation: 1,
            },
        ),
        project_id: Some("p".into()),
        command_class: Some(cairn_common::executor_protocol::CellCommandClass::classify(
            command,
        )),
    };
    let old = kill(11, now - 3 * DAY, "cargo test -p cairn-core");
    let new = kill(12, now, "bun run test:frontend");
    {
        let health = Arc::make_mut(&mut entry.health);
        health.machine.memory = Measurement::measured(
            now,
            MachineMemory {
                total_bytes: 13_000_000_000,
                available_bytes: 9 * GIB,
            },
        );
        health.memory_record.standing_bytes = Some(GIB);
        health.memory_record.oom_kills = vec![old.clone(), new];
    }
    let selected = selected_executor(&entry);
    let context = ReservationPlan::profile_context(&selected);
    pool.lock_connections().insert(id, entry);
    // Day one: A's kill was learned when it happened.
    let suite_a = resource_profiles::suite_key(
        "p",
        None,
        cairn_common::executor_protocol::CellCommandClass::CargoTest,
    )
    .unwrap();
    resource_profiles::observe_suite_kill(
        db.clone(),
        &suite_a,
        &context,
        &old,
        Some(13_000_000_000),
        false,
    )
    .await;
    for _ in 0..3 {
        pool.learn_resident_kills(db.clone(), &selected.executor_id, &context)
            .await;
    }
    let counts = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT suite_key, oom_crowded_kill_count FROM suite_memory_evidence ORDER BY suite_key",
                        (),
                    )
                    .await?;
                let mut found = Vec::new();
                while let Some(row) = rows.next().await? {
                    found.push((row.get::<String>(0)?, row.get::<i64>(1)?));
                }
                Ok(found)
            })
        })
        .await
        .unwrap();
    assert_eq!(
        counts,
        vec![
            ("p/class:cargoTest".to_string(), 1),
            ("p/class:vitest".to_string(), 1)
        ]
    );
    assert_eq!(
        pool.resident_command_misfit(db, &selected, "p", Some("cargo test -p cairn-core"))
            .await,
        None,
        "one kill below the fit line still fits"
    );
}

/// Work whose process fits only machines that are attached but away for now
/// (here: drained) waits for one of them, rather than being refused as
/// though no machine could ever hold it.
///
/// Red since the dispatcher took placement over: `refusal_at_join` refuses
/// when every candidate is excluded for size, without asking whether a
/// machine it filtered out as away for now would hold the work. The old
/// placement loop parked on the size exclusion instead.
#[tokio::test]
async fn a_process_whose_only_fitting_machine_is_drained_waits_for_it() {
    const GIB: u64 = 1024 * 1024 * 1024;
    let (pool, db, request) = process_fit_fleet("process-fit-drained.db", 125 * GIB, true).await;
    announce_posted_rooms(&pool);
    let plan = Arc::new(ReservationPlan::new(db, &request, None));
    let placing = tokio::spawn({
        let pool = pool.clone();
        async move { place_through_dispatcher(&pool, &request, Some(plan)).await }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    if placing.is_finished() {
        panic!(
            "a drained machine that fits is worth waiting for: {:?}",
            placing.await
        );
    }

    // The drain lifts: the machine's facts moved, and the room it already
    // announced takes the work.
    pool.lock_connections().get_mut("other").unwrap().drained = false;
    pool.machine_facts_posted("other");
    let placement = tokio::time::timeout(Duration::from_secs(5), placing)
        .await
        .expect("placed once the drain lifts")
        .expect("the placing task ends")
        .expect("the undrained machine takes it");
    assert_eq!(placement.selected.executor_id, "other");
}

/// The issue's test end to end: a never-seen identity of a suite whose
/// rustc is 10 GiB is not offered to a 13 GiB machine and is offered to a
/// 125 GiB one; when every machine is too small it is refused at once
/// rather than waiting out its horizon.
#[tokio::test]
async fn a_process_larger_than_a_machine_is_placed_elsewhere_or_refused() {
    const GIB: u64 = 1024 * 1024 * 1024;
    let select = |pool: Fleet, db, request: CellRequest| async move {
        announce_posted_rooms(&pool);
        let plan = Arc::new(ReservationPlan::new(db, &request, None));
        tokio::time::timeout(
            Duration::from_secs(2),
            place_through_dispatcher(&pool, &request, Some(plan)),
        )
        .await
    };

    let (pool, db, request) = process_fit_fleet("process-fit-elsewhere.db", 125 * GIB, false).await;
    let placement = select(pool, db, request)
        .await
        .expect("assigned at once")
        .expect("the large machine takes it");
    assert_eq!(placement.selected.executor_id, "other");

    let (pool, db, request) = process_fit_fleet("process-fit-nowhere.db", 14 * GIB, false).await;
    let refusal = select(pool, db, request)
        .await
        .expect("refused at once")
        .expect_err("no machine can give one process 12.5 GiB");
    let CellOutcome::Unavailable { diagnostic, .. } = refusal else {
        panic!("a refusal, got {refusal:?}");
    };
    assert!(
        diagnostic.contains("no attached machine can hold one process of this work"),
        "{diagnostic}"
    );
    assert!(
        diagnostic.contains("bgsrv-ub: needs one process"),
        "{diagnostic}"
    );
}

/// The room a machine of `total_bytes` announces with `free_cores` of its
/// 64 cores not given out and all but 3 GiB of its memory available.
fn sized_room(total_bytes: u64, free_cores: u64) -> cairn_common::executor_protocol::AnnouncedRoom {
    use cairn_common::executor_protocol::{
        Allowance, AnnouncedRoom, CompileSlots, DiskRoom, MemoryRoom,
    };
    const GIB: u64 = 1 << 30;
    AnnouncedRoom {
        slots: CompileSlots::of(64),
        compiling: u32::try_from((64 - free_cores).div_ceil(8)).unwrap(),
        test_cores: 0,
        standing: Default::default(),
        cores: Allowance {
            limit: 64,
            committed: 64 - free_cores,
        },
        memory: Some(MemoryRoom {
            available_bytes: total_bytes - 3 * GIB,
            owed_bytes: 0,
            total_bytes,
        }),
        memory_budget: None,
        disk: Some(DiskRoom {
            free_bytes: 10_000 * GIB,
            watermark_bytes: GIB,
            reserved_bytes: 0,
            inodes: None,
        }),
        disk_budget: None,
        reclaimable_memory_bytes: 0,
        alone: None,
        provisioning: Vec::new(),
        holds: Vec::new(),
        closures: Vec::new(),
    }
}

/// A resident transition during the initial figure must change both admission
/// and the reservation offered, before the entry can be assigned or refused.
#[tokio::test]
async fn joining_compile_refigures_when_residents_start_or_stop() {
    use cairn_common::executor_protocol::CompileSlots;
    const GIB: u64 = 1 << 30;
    let db = Arc::new(crate::storage::migrated_test_db("join-resident-width.db").await);
    for (before, after) in [(0, 2), (2, 0)] {
        let pool = Fleet::default();
        let now = unix_time_ms();
        let (id, mut machine) = fleet_entry_at("compile", "linux", 0, &[], now);
        Arc::make_mut(&mut machine.advertisement)
            .capabilities
            .logical_cores = 12;
        measured_at(&mut machine, 0.01, 120 * GIB, 500 * GIB, now);
        pool.lock_connections().insert(id, machine);
        let room = |standing| {
            let mut room = sized_room(128 * GIB, 64);
            room.slots = CompileSlots::net(12, standing);
            room.cores.limit = 12;
            room.cores.committed = u64::from(standing);
            room.standing.cores = standing;
            room
        };
        pool.record_room("compile", 1, 1, room(before));
        let next = room(after);
        pool.after_figure_snapshot_for_test(Box::new(move |pool| {
            Box::pin(async move {
                pool.record_room("compile", 1, 2, next);
            })
        }));
        let mut request = spillable_request();
        request.command_class = CellCommandClass::CargoCheck;
        request.wait_horizon_unix_ms = now + 30_000;
        let plan = Arc::new(ReservationPlan::new(db.clone(), &request, None));
        let placement = tokio::time::timeout(
            Duration::from_secs(5),
            place_through_dispatcher(&pool, &request, Some(plan)),
        )
        .await
        .expect("joins without waiting for another room")
        .expect("the current slot fits");
        assert_eq!(
            placement.reservation.unwrap().reservation.concurrency_units,
            12 - after,
            "resident cores changed from {before} to {after}"
        );
    }
}

/// Queue `request` and join it to the dispatcher, keeping its channel.
async fn joined(
    pool: &Fleet,
    request: &CellRequest,
    plan: Arc<ReservationPlan>,
) -> mpsc::UnboundedReceiver<super::assignments::Delivery> {
    let (entry, waiter) = pool
        .queue
        .enqueue(
            queue::NewEntry {
                request: request.clone(),
                admission_kind: CellAdmissionKind::Command,
                interactive: false,
                execution_residency: None,
                standing: false,
            },
            unix_time_ms(),
        )
        .unwrap();
    std::mem::forget(waiter);
    pool.join_dispatcher(
        &entry,
        request,
        Some(plan),
        &ActivePlacementPolicy::default_profile(),
    )
    .await
    .unwrap_or_else(|outcome| panic!("waits rather than {outcome:?}"))
}

/// The machine the dispatcher next assigns from `deliveries`, or `None`
/// when it assigns nothing within half a second.
async fn assigned_within(
    deliveries: &mut mpsc::UnboundedReceiver<super::assignments::Delivery>,
) -> Option<String> {
    match tokio::time::timeout(Duration::from_millis(500), deliveries.recv()).await {
        Err(_) => None,
        Ok(Some(super::assignments::Delivery::Assigned(assignment))) => {
            Some(assignment.executor_id)
        }
        Ok(other) => panic!("an assignment or nothing, not {other:?}"),
    }
}

/// A cold compile, nothing of it measured, in a project whose rust-tests
/// suite peaked at a 9.9 GB rustc: that class prior is more than bgsrv-ub
/// (12.1 GiB) can give one process. With the 62 GiB machine busy and
/// bgsrv-ub idle, the work waits for the large machine rather than being
/// bgsrv-ub's first measurement of it, and the account says so naming the
/// machine waited for; the large machine takes it when its cores free.
/// (Main's ranking queued it on the busy machine; cairn/7339 S3.)
#[tokio::test]
async fn a_cold_compile_waits_for_a_busy_machine_that_holds_its_class_prior() {
    const GIB: u64 = 1024 * 1024 * 1024;
    const SMALL: u64 = 12_992_276_070;
    let db = Arc::new(crate::storage::migrated_test_db("process-prior-busy.db").await);
    let pool = Fleet::default();
    let now = unix_time_ms();
    let mut connections = HashMap::new();
    let mut small = None;
    for (id, total) in [("bgsrv-ub", SMALL), ("bbs", 62 * GIB)] {
        let (id, mut entry) = fleet_entry_at(id, "linux", 0, &[], now);
        Arc::make_mut(&mut entry.advertisement)
            .capabilities
            .memory_budget_bytes = Some(total / 100 * 85);
        measured_at(&mut entry, 0.01, total - 3 * GIB, 500 * GIB, now);
        Arc::make_mut(&mut entry.health).machine.memory = Measurement::measured(
            now,
            MachineMemory {
                total_bytes: total,
                available_bytes: total - 3 * GIB,
            },
        );
        Arc::make_mut(&mut entry.health)
            .memory_record
            .standing_bytes = Some(5 * GIB / 2);
        small.get_or_insert_with(|| {
            ReservationPlan::context_for(
                &entry.identity.device_id,
                &entry.identity.executor_id,
                &entry.advertisement.capabilities,
            )
        });
        connections.insert(id, entry);
    }
    *pool.lock_connections() = connections;
    let mut request = spillable_request();
    request.command_class = CellCommandClass::CargoCheck;
    request.wait_horizon_unix_ms = now + 600_000;
    resource_profiles::observe_suite(
        db.clone(),
        Some(&format!("{}/rust-tests", request.project_id)),
        CellCommandClass::CargoTest,
        &small.unwrap(),
        &CellExecutionMeta {
            stop: None,
            peak_process: Some(cairn_common::executor_protocol::PeakProcess {
                bytes: 9_910_000_000,
                name: Some("rustc".into()),
            }),
            ..resource_profiles::oom_killed_for_test(GIB, None, 0, 1)
        },
        resource_profiles::CompletionKind::Failed,
        resource_profiles::MachineAdmission::default(),
        resource_profiles::CompletedSource::default(),
    )
    .await;
    request.command_resource_identity = Some(CommandResourceIdentity {
        version: 1,
        key: "workspace-compile:cold".into(),
        suite: Some("workspace-compile".into()),
    });
    pool.record_room("bbs", 1, 1, sized_room(62 * GIB, 0));
    pool.record_room("bgsrv-ub", 1, 1, sized_room(SMALL, 64));
    let plan = Arc::new(ReservationPlan::new(db, &request, None));
    let mut deliveries = joined(&pool, &request, plan).await;
    assert_eq!(assigned_within(&mut deliveries).await, None);

    let Some(CheckDispatchPhase::Placing {
        declines,
        placement: Some(account),
        turn,
        ..
    }) = pool.queued_request_account(&request.request_id)
    else {
        panic!("a queued request in the dispatcher has an account");
    };
    let ub = account
        .machines
        .iter()
        .find(|machine| machine.executor_name == "bgsrv-ub")
        .expect("bgsrv-ub is in the account");
    assert_eq!(status_kind(&ub.status), "waits for");
    let held =
        crate::execution::checks::describe_queued_hold(&declines, Some(&account), turn.as_ref())
            .unwrap();
    assert!(
        held.contains("bgsrv-ub: its room #1 fits, but the work waits for bbs"),
        "{held}"
    );

    pool.record_room("bbs", 1, 2, sized_room(62 * GIB, 64));
    assert_eq!(
        assigned_within(&mut deliveries).await.as_deref(),
        Some("bbs")
    );
}

/// A learned peak above bgsrv-ub's ceiling: placed there it runs at the
/// clamp. While a busy machine whose budget holds the peak is attached,
/// work free to move waits for it; pinned to bgsrv-ub it runs there at
/// once; and with no machine able to hold the peak it runs at the clamp at
/// once, since waiting buys nothing (cairn/5731; cairn/7339 S3).
#[tokio::test]
async fn a_clamped_peak_waits_only_for_a_machine_that_could_hold_it() {
    const GIB: u64 = 1024 * 1024 * 1024;
    const UB_BUDGET: u64 = 11_042_782_208;
    for (case, other_budget, pinned, waits) in [
        ("holdable elsewhere", 100 * GIB, false, true),
        ("pinned home", 100 * GIB, true, false),
        ("nowhere larger", UB_BUDGET, false, false),
    ] {
        let db = Arc::new(
            crate::storage::migrated_test_db(&format!("ceiling-{}.db", case.replace(' ', "-")))
                .await,
        );
        let identity = CommandResourceIdentity {
            version: 1,
            key: "test:rust".into(),
            suite: None,
        };
        let pool = Fleet::default();
        let now = unix_time_ms();
        let mut connections = HashMap::new();
        for (id, budget) in [("bgsrv-ub", UB_BUDGET), ("large", other_budget)] {
            let (id, mut entry) = fleet_entry_at(id, "linux", 0, &[], now);
            Arc::make_mut(&mut entry.advertisement)
                .capabilities
                .memory_budget_bytes = Some(budget);
            measured_at(&mut entry, 0.01, 10 * GIB, 500 * GIB, now);
            resource_profiles::seed_memory_profile_for_test(
                db.clone(),
                &identity,
                &ReservationPlan::context_for(
                    &entry.identity.device_id,
                    &entry.identity.executor_id,
                    &entry.advertisement.capabilities,
                ),
                8 * GIB,
            )
            .await;
            connections.insert(id, entry);
        }
        *pool.lock_connections() = connections;
        let mut request = spillable_request();
        request.command_class = CellCommandClass::CargoTest;
        request.command_resource_identity = Some(identity);
        request.wait_horizon_unix_ms = now + 600_000;
        if pinned {
            request.pinned_executor_id = Some("bgsrv-ub".into());
        }
        pool.record_room("large", 1, 1, sized_room(128 * GIB, 0));
        pool.record_room("bgsrv-ub", 1, 1, sized_room(64 * GIB, 64));
        let plan = Arc::new(ReservationPlan::new(db, &request, None));
        let mut deliveries = joined(&pool, &request, plan).await;
        let first = assigned_within(&mut deliveries).await;
        if !waits {
            assert_eq!(first.as_deref(), Some("bgsrv-ub"), "{case}");
            continue;
        }
        assert_eq!(first, None, "{case}");
        pool.record_room("large", 1, 2, sized_room(128 * GIB, 64));
        assert_eq!(
            assigned_within(&mut deliveries).await.as_deref(),
            Some("large"),
            "{case}"
        );
    }
}

/// bgsrv-ub, freshly killed for memory, beside fedora, which has never run
/// the command. Placement after the kill is the question.
async fn killed_fleet(
    db_name: &str,
    ub_available: u64,
    co_resident_compile_jobs: u32,
) -> (Fleet, Arc<cairn_db::storage::LocalDb>, CellRequest) {
    let db = Arc::new(crate::storage::migrated_test_db(db_name).await);
    let (pool, request) =
        killed_fleet_in(db.clone(), ub_available, co_resident_compile_jobs, true).await;
    (pool, db, request)
}

/// [`killed_fleet`] over `db`. Without `record_kill` it is a runner booting
/// onto a ledger an earlier one wrote.
async fn killed_fleet_in(
    db: Arc<cairn_db::storage::LocalDb>,
    ub_available: u64,
    co_resident_compile_jobs: u32,
    record_kill: bool,
) -> (Fleet, CellRequest) {
    const GIB: u64 = 1024 * 1024 * 1024;
    const UB_BUDGET: u64 = 11_042_782_208;
    const UB_TOTAL: u64 = 13_000_000_000;
    let identity = CommandResourceIdentity {
        version: 1,
        key: "test:rust".into(),
        suite: None,
    };
    let pool = Fleet::default();
    let now = unix_time_ms();
    let mut connections = HashMap::new();
    for (id, budget, available) in [
        ("bgsrv-ub", UB_BUDGET, ub_available),
        ("fedora", 100 * GIB, 60 * GIB),
    ] {
        let (id, mut entry) = fleet_entry_at(id, "linux", 0, &[], now);
        Arc::make_mut(&mut entry.advertisement)
            .capabilities
            .memory_budget_bytes = Some(budget);
        measured_at(&mut entry, 0.01, available, 500 * GIB, now);
        if id == "bgsrv-ub" {
            // The size it had when the kernel killed the command there.
            Arc::make_mut(&mut entry.health).machine.memory = Measurement::measured(
                now,
                cairn_common::executor_protocol::MachineMemory {
                    total_bytes: UB_TOTAL,
                    available_bytes: available,
                },
            );
        }
        connections.insert(id, entry);
    }
    let ub = &connections["bgsrv-ub"];
    if record_kill {
        resource_profiles::observe_memory_kill(
            db.clone(),
            Some(&identity),
            &ReservationPlan::context_for(
                &ub.identity.device_id,
                &ub.identity.executor_id,
                &ub.advertisement.capabilities,
            ),
            &resource_profiles::oom_killed_for_test(
                3_650_722_201,
                Some(UB_TOTAL),
                co_resident_compile_jobs,
                now,
            ),
        )
        .await;
    }
    *pool.lock_connections() = connections;
    let mut request = spillable_request();
    request.command_class = cairn_common::executor_protocol::CellCommandClass::CargoTest;
    request.command_resource_identity = Some(identity);
    request.repository = RepositoryLocator::ManagedObjects {
        project_id: "p".into(),
        repository_id: "repo".into(),
        object_format: GitObjectFormat::Sha1,
    };
    (pool, request)
}

async fn place_once(
    pool: &Fleet,
    db: Arc<cairn_db::storage::LocalDb>,
    request: &CellRequest,
) -> Placement {
    announce_posted_rooms(pool);
    let plan = Arc::new(ReservationPlan::new(db, request, None));
    tokio::time::timeout(
        Duration::from_secs(5),
        place_through_dispatcher(pool, request, Some(plan)),
    )
    .await
    .expect("assigned at once")
    .expect("a machine takes it")
}

/// cairn/5416's specimen: a `rustc` killed at 3.4 GiB beside another
/// compile on a 12.1 GiB machine now showing 2.5 GiB free. The re-armed
/// lane is charged the kill, not the 2 GiB class prior that fit, so it goes
/// to the machine that can take it. The charge is the victim's own floor: a compile runs at its one
/// slot width, so there is no per-job model to narrow it and no width pricing
/// to raise it (cairn/7303).
#[tokio::test]
async fn a_command_killed_for_memory_is_not_placed_back_where_it_does_not_fit() {
    let available = 2_684_354_560;
    let (pool, db, request) = killed_fleet("oom-floor-placement.db", available, 1).await;
    let placement = place_once(&pool, db, &request).await;
    assert_eq!(placement.selected.executor_id, "fedora");
    assert!(
        placement
            .decision
            .rejected
            .iter()
            .any(|rejection| rejection.executor_id == "bgsrv-ub"),
        "the decision names the machine the command was killed on: {:?}",
        placement.decision.rejected
    );
}

/// A command's start-load isolation cannot veto an otherwise safe candidate.
#[tokio::test]
async fn a_command_killed_with_zero_start_load_remains_a_candidate() {
    const GIB: u64 = 1 << 30;
    let (pool, db, request) = killed_fleet("oom-alone-placement.db", 10 * GIB, 0).await;
    pool.lock_connections().remove("fedora");
    let placement = place_once(&pool, db, &request).await;
    assert_eq!(placement.selected.executor_id, "bgsrv-ub");
    assert!(placement.above_ceiling.is_none());
    assert!(
        placement
            .reservation
            .as_ref()
            .unwrap()
            .reservation
            .memory_bytes
            >= 3_650_722_201
    );
}

/// Reopening persisted kill evidence applies the same causal policy immediately.
#[tokio::test]
async fn a_runner_booting_onto_a_kill_ledger_retires_the_false_host_exclusion() {
    const GIB: u64 = 1 << 30;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("oom-boot.db");
    {
        let earlier = Arc::new(
            cairn_db::storage::LocalDb::open_migrated_for_test(path.clone())
                .await
                .unwrap(),
        );
        killed_fleet_in(earlier, 10 * GIB, 0, true).await;
    }
    let db = Arc::new(
        cairn_db::storage::LocalDb::open_migrated_for_test(path)
            .await
            .unwrap(),
    );
    let (pool, request) = killed_fleet_in(db.clone(), 10 * GIB, 0, false).await;
    pool.lock_connections().remove("fedora");
    let placement = place_once(&pool, db, &request).await;
    assert_eq!(placement.selected.executor_id, "bgsrv-ub");
    assert!(
        placement
            .reservation
            .as_ref()
            .unwrap()
            .reservation
            .memory_bytes
            >= 3_650_722_201
    );
}

/// What placement can tell about `request` on this fleet before any room is
/// read: the one machine it could go to, when only one could, and every
/// machine passed over with its reason. Which of several eligible machines
/// takes the work is the dispatcher's, by announced room.
#[derive(Debug)]
struct PlacementDraft {
    selected: Option<(SelectedExecutor, PlacementSelection)>,
    rejected: Vec<PlacementRejection>,
}

fn place(
    connections: &HashMap<String, ExecutorConnectionState>,
    request: &CellRequest,
) -> Result<PlacementDraft, String> {
    let survey =
        survey_candidates(connections, &[], request, NOW).map_err(|failure| failure.diagnostic)?;
    let selected = match survey.usable.as_slice() {
        [only] => Some((selected_executor(only), sole_selection(only, request))),
        _ => None,
    };
    Ok(PlacementDraft {
        selected,
        rejected: survey.rejected,
    })
}

/// The selection record of the only machine that could take `request`.
fn sole_selection(entry: &ExecutorConnectionState, request: &CellRequest) -> PlacementSelection {
    PlacementSelection {
        executor_name: executor_public_name(entry),
        executor_id: entry.identity.executor_id.clone(),
        colocated: entry.colocated,
        reason: PlacementReason::OnlyCandidate,
        tie_break: None,
        readings: super::selection::placement_readings(&entry.health.machine),
        reservation: request.resource_reservation.clone(),
        reservation_rationale: unresolved_rationale(&request.resource_reservation),
        sync_cost: PlacementSyncCost::Unknown,
        object_transfer: None,
        observation_reuse: if entry.colocated {
            ObservationReuse::Colocated
        } else {
            ObservationReuse::UntrustedRemoteEnvironment
        },
        prediction: None,
        memory_ceiling: None,
        process_fit_waived: None,
        nested_sandbox_waived: None,
        restart_exposure: None,
        process_demand: None,
        cache_distance: None,
    }
}

/// The one machine `request` could go to on this fleet, `None` when none
/// could, or the refusal owed at once.
fn choose_executor(
    connections: &HashMap<String, ExecutorConnectionState>,
    request: &CellRequest,
) -> Result<Option<SelectedExecutor>, String> {
    Ok(place(connections, request)?
        .selected
        .map(|(selected, _)| selected))
}

/// Every attached machine announces a roomy room, as a real executor does
/// when it attaches.
fn announce_every_room(pool: &Fleet) {
    let machines: Vec<(String, u64)> = pool
        .lock_connections()
        .iter()
        .map(|(id, entry)| (id.clone(), entry.generation))
        .collect();
    for (id, generation) in machines {
        pool.announce_room_for_test(&id, generation);
    }
}

/// Every attached machine announces the memory room its posted reading
/// gives it, the way its own admission would: the room is what decides
/// whether work fits there.
fn announce_posted_rooms(pool: &Fleet) {
    /// A machine, its link, and its posted (available, total) memory.
    type Posted = (String, u64, Option<(u64, u64)>);
    let machines: Vec<Posted> = pool
        .lock_connections()
        .iter()
        .map(|(id, entry)| {
            (
                id.clone(),
                entry.generation,
                entry
                    .health
                    .machine
                    .memory
                    .value()
                    .map(|memory| (memory.available_bytes, memory.total_bytes)),
            )
        })
        .collect();
    for (id, generation, memory) in machines {
        pool.announce_room_with_memory_for_test(&id, generation, memory);
    }
}

/// Wait for the dispatcher to assign the queued `entry_id` a machine, as
/// `execute_entry` does, leaving the dispatcher on the way out.
// The outcome owed, as `await_assignment` returns it.
#[allow(clippy::result_large_err)]
async fn assign(
    pool: &Fleet,
    entry_id: &str,
    request: &CellRequest,
    policy: &ActivePlacementPolicy,
    plan: Option<Arc<ReservationPlan>>,
) -> Result<Placement, CellOutcome> {
    let _joined = super::assignments::JoinedDispatcher {
        pool: pool.clone(),
        entry_id: entry_id.to_string(),
    };
    pool.await_assignment(entry_id, request, plan.as_ref(), policy, &mut None)
        .await
}

/// Queue `request` and wait for the dispatcher to assign it a machine.
#[allow(clippy::result_large_err)]
async fn place_through_dispatcher(
    pool: &Fleet,
    request: &CellRequest,
    plan: Option<Arc<ReservationPlan>>,
) -> Result<Placement, CellOutcome> {
    let (entry_id, _waiter) = pool
        .queue
        .enqueue(
            queue::NewEntry {
                request: request.clone(),
                admission_kind: CellAdmissionKind::Command,
                interactive: false,
                execution_residency: None,
                standing: false,
            },
            unix_time_ms(),
        )
        .expect("the request enqueues");
    assign(
        pool,
        &entry_id,
        request,
        &ActivePlacementPolicy::default_profile(),
        plan,
    )
    .await
}

/// A check lane's verdict is reproducible against the branch alone, so the
/// batch every check submits carries no node scratch.
#[test]
fn a_check_lane_batch_carries_no_node_scratch() {
    let batch = Fleet::check_process_batch(Vec::new(), Some("context".into()), None, 0);
    assert!(batch.node_scratch.is_none());
}

fn chosen(draft: &PlacementDraft) -> &PlacementSelection {
    &draft.selected.as_ref().expect("a machine was chosen").1
}

fn rejection_for<'a>(draft: &'a PlacementDraft, executor_id: &str) -> &'a PlacementRejectionReason {
    &draft
        .rejected
        .iter()
        .find(|rejection| rejection.executor_id == executor_id)
        .unwrap_or_else(|| panic!("{executor_id} was evaluated"))
        .reason
}

const GIB: u64 = 1024 * 1024 * 1024;

/// Untargeted is not the same property as free to move. An agent's own batch
/// states no selector and is still bound to the machine holding its tree.
#[test]
fn conservative_untargeted_work_never_leaves_the_colocated_executor() {
    let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]);
    measured(&mut local, 0.99, 1024 * 1024 * 1024, 1024 * 1024 * 1024);
    let (remote_id, mut remote) = fleet_entry("bglab-ub", "linux", 0, &[]);
    measured(
        &mut remote,
        0.01,
        48 * 1024 * 1024 * 1024,
        900 * 1024 * 1024 * 1024,
    );
    let connections = HashMap::from([(local_id, local), (remote_id, remote)]);

    let mut request = spillable_request();
    request.placement_mobility = PlacementMobility::PinnedOrColocated;
    let draft = place(&connections, &request).unwrap();
    assert_eq!(chosen(&draft).executor_id, COLOCATED_EXECUTOR_ID);
    assert_eq!(
        rejection_for(&draft, "bglab-ub"),
        &PlacementRejectionReason::NotColocated
    );
}

#[test]
fn unpinned_job_residency_uses_transferable_placement_identity() {
    let acquisition = fleet_residency_request(ResidencyHolder::Job {
        job_id: "job".into(),
    });
    let placement = residency_placement_request(&acquisition);
    assert!(matches!(
        placement.repository,
        RepositoryLocator::ManagedObjects { .. }
    ));
    assert_eq!(
        placement.placement_mobility,
        PlacementMobility::SpillEligible
    );
    assert!(matches!(
        acquisition.repository,
        RepositoryLocator::ColocatedPath { .. }
    ));
}

#[test]
fn dev_instance_residency_remains_colocated_and_nontransferable() {
    let placement = residency_placement_request(&dev_instance_request("dev"));
    assert!(matches!(
        placement.repository,
        RepositoryLocator::ColocatedPath { .. }
    ));
    assert_eq!(
        placement.pinned_executor_id.as_deref(),
        Some(COLOCATED_EXECUTOR_ID)
    );
    assert_eq!(
        placement.placement_mobility,
        PlacementMobility::PinnedOrColocated
    );
}

#[test]
fn residency_dispatch_fences_the_selected_connection_generation() {
    let (id, entry) = fleet_entry("fedora", "linux", 0, &[]);
    let selected = SelectedExecutor {
        executor_id: id.clone(),
        device_id: entry.identity.device_id.clone(),
        generation: entry.generation,
        sender: entry.sender.clone(),
        colocated: false,
        capabilities: entry.advertisement.capabilities.clone(),
    };
    let mut reconnected = entry;
    reconnected.generation += 1;
    let error =
        selected_link_dispatch_error(&HashMap::from([(id, reconnected)]), &selected, NOW).unwrap();
    assert!(error.contains("old generation is fenced"), "{error}");
}

#[test]
fn stale_colocated_link_is_rejected_before_blind_ranking_and_healthy_remote_wins() {
    let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]);
    local.last_progress_unix_ms = NOW - EXECUTOR_PROGRESS_FRESHNESS_MS - 1;
    let (remote_id, mut remote) = fleet_entry("fedora", "linux", 0, &[]);
    measured(
        &mut remote,
        0.87,
        8 * 1024 * 1024 * 1024,
        500 * 1024 * 1024 * 1024,
    );
    let connections = HashMap::from([(local_id, local), (remote_id, remote)]);

    let draft = place(&connections, &spillable_request()).unwrap();
    assert_eq!(chosen(&draft).executor_id, "fedora");
    assert!(matches!(
        rejection_for(&draft, COLOCATED_EXECUTOR_ID),
        PlacementRejectionReason::ConnectionStale { .. }
    ));
}

#[test]
fn pinned_stale_executor_reports_link_unavailability_instead_of_rehoming() {
    let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]);
    local.last_progress_unix_ms = NOW - EXECUTOR_PROGRESS_FRESHNESS_MS - 1;
    let (remote_id, remote) = fleet_entry("fedora", "linux", 0, &[]);
    let connections = HashMap::from([(local_id, local), (remote_id, remote)]);
    let mut request = spillable_request();
    request.pinned_executor_id = Some(COLOCATED_EXECUTOR_ID.into());

    let error = place(&connections, &request).unwrap_err();
    assert!(error.contains("local"), "{error}");
    assert!(error.contains("published no progress"), "{error}");
}

fn pipes_reading(entry: &mut ExecutorConnectionState, at_unix_ms: u64, accepted_bytes: u64) {
    let health = Arc::make_mut(&mut entry.health);
    let evidence = health
        .host
        .pipe_health
        .get_or_insert_with(|| cairn_common::pipe_health::PipeHealth::new(true));
    evidence.observe(Measurement::measured(
        at_unix_ms,
        cairn_common::pipe_health::PipeCapacity {
            accepted_bytes,
            at_least: false,
        },
    ));
}

/// cairn/6023: a machine whose fresh pipes hold 512 B is passed over for
/// a healthy one, binds its own pins rather than being waited on, and is
/// never held shut by evidence that has outlived the bounded stale hold.
#[test]
fn a_pipe_degraded_machine_is_passed_over_and_binds_its_pins() {
    use cairn_common::pipe_health::{FRESH_FOR_MS, STALE_HOLD_MS};
    let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]);
    measured(
        &mut local,
        0.01,
        48 * 1024 * 1024 * 1024,
        900 * 1024 * 1024 * 1024,
    );
    let (remote_id, mut remote) = fleet_entry("bglab-ub", "linux", 0, &[]);
    measured(
        &mut remote,
        0.85,
        8 * 1024 * 1024 * 1024,
        500 * 1024 * 1024 * 1024,
    );
    assert!(
        pipe_health_rejection(&local, PipeFloorWork::Process, NOW).is_none(),
        "no evidence is a named gap that admits"
    );
    pipes_reading(&mut local, NOW, 512);
    let connections = HashMap::from([(local_id, local), (remote_id, remote)]);

    let draft = place(&connections, &spillable_request()).unwrap();
    assert_eq!(chosen(&draft).executor_id, "bglab-ub");
    let PlacementRejectionReason::PipeDegraded { assessment } =
        rejection_for(&draft, COLOCATED_EXECUTOR_ID)
    else {
        panic!("the degraded machine is rejected for its pipes");
    };
    assert_eq!(
        assessment.capacity.map(|capacity| capacity.accepted_bytes),
        Some(512)
    );
    assert!(!waits_on_machine(&PlacementRejectionReason::PipeDegraded {
        assessment: assessment.clone()
    }));

    let mut pinned = spillable_request();
    pinned.pinned_executor_id = Some(COLOCATED_EXECUTOR_ID.into());
    pinned.placement_mobility = PlacementMobility::PinnedOrColocated;
    assert!(matches!(
        candidate_rejection(
            &connections[COLOCATED_EXECUTOR_ID],
            PlacementScope::of(&pinned),
            NOW
        ),
        Some(PlacementRejectionReason::PipeDegraded { .. })
    ));

    let local = &connections[COLOCATED_EXECUTOR_ID];
    assert!(matches!(
        pipe_health_rejection(local, PipeFloorWork::Process, NOW + FRESH_FOR_MS + 1),
        Some(PlacementRejectionReason::PipeDegraded { assessment })
            if assessment.admission == cairn_common::pipe_health::PipeAdmission::StaleHold
    ));
    assert!(
        pipe_health_rejection(
            local,
            PipeFloorWork::Process,
            NOW + FRESH_FOR_MS + STALE_HOLD_MS
        )
        .is_none(),
        "expired evidence admits with a gap rather than refusing forever"
    );
}

/// cairn/6440: placement mirrors the executor's short-command rule. An
/// agent's command batch is placed on a machine in recovery hysteresis and
/// refused while its pipe reads below the floor; a check lane and a
/// residency acquisition are refused in both phases.
#[test]
fn an_agent_command_passes_recovery_hysteresis_and_nothing_else_does() {
    let (_, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "macos", 0, &[]);
    pipes_reading(&mut local, NOW, 512);
    let mut command = spillable_request();
    command.priority = CellPriority::AgentInteractive;
    let mut check = spillable_request();
    check.priority = CellPriority::ReviewCheck;
    let mut acquisition = command.clone();
    acquisition.attempt_id = RESIDENCY_ACQUIRE_ATTEMPT_ID.into();
    assert_eq!(pipe_floor_work(&command), PipeFloorWork::ShortCommand);
    assert_eq!(pipe_floor_work(&check), PipeFloorWork::Process);
    assert_eq!(pipe_floor_work(&acquisition), PipeFloorWork::Process);

    let rejected = |entry: &ExecutorConnectionState, request: &CellRequest, at| {
        matches!(
            candidate_rejection(entry, PlacementScope::of(request), at),
            Some(PlacementRejectionReason::PipeDegraded { .. })
        )
    };
    // Degraded: everything is refused.
    for request in [&command, &check, &acquisition] {
        assert!(rejected(&local, request, NOW));
    }
    // Recovering at 65536 B: only the short command is placed.
    pipes_reading(&mut local, NOW + 5_000, 65_536);
    let at = NOW + 5_000;
    assert!(!rejected(&local, &command, at));
    assert!(rejected(&local, &check, at));
    assert!(rejected(&local, &acquisition, at));
}

/// The CAIRN-3452 specimen, as placement sees it.
///
/// A review wave with nowhere idle to go locally found one measured, idle
/// Windows machine and took it, and the target-gated dead-code findings
/// clippy only emits there were recorded as the PR's rust-lint verdict.
/// Every input placement ranks on said bglab-win was the better machine.
/// The one thing that makes it the wrong machine is not a reading at all:
/// this project's gate has only ever meant green on macOS, so a Windows
/// answer is not this check's answer however fast it arrives.
#[test]
fn a_verdict_never_travels_to_a_platform_the_project_does_not_gate_on() {
    let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "macos", 0, &[]);
    measured(
        &mut local,
        0.97,
        2 * 1024 * 1024 * 1024,
        100 * 1024 * 1024 * 1024,
    );
    let (windows_id, mut windows) = fleet_entry("bglab-win", "windows", 0, &[]);
    measured(
        &mut windows,
        0.01,
        64 * 1024 * 1024 * 1024,
        900 * 1024 * 1024 * 1024,
    );
    let connections = HashMap::from([(local_id, local), (windows_id, windows)]);

    let mut request = spillable_request();
    request.verdict_platforms = vec!["macos".into()];
    let draft = place(&connections, &request).unwrap();
    assert_eq!(
        chosen(&draft).executor_id,
        COLOCATED_EXECUTOR_ID,
        "a busy machine the project gates on beats an idle one it does not"
    );
    let rejection = rejection_for(&draft, "bglab-win");
    assert!(
        matches!(
            rejection,
            PlacementRejectionReason::UntrustedVerdictPlatform { .. }
        ),
        "the reason must be the trust, not a reading: {rejection:?}"
    );
    assert!(rejection.describe().contains("windows"), "{rejection:?}");
}

/// A batch that cannot move has one possible machine, and waiting cannot
/// change its platform. That is a contradiction between what a check
/// declared and the cadence it runs at, and it is answered immediately
/// rather than by stalling out the horizon on every commit.
#[test]
fn work_that_cannot_move_refuses_at_once_when_its_home_is_not_gated_on() {
    let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "macos", 0, &[]);
    measured(
        &mut local,
        0.10,
        8 * 1024 * 1024 * 1024,
        500 * 1024 * 1024 * 1024,
    );
    let connections = HashMap::from([(local_id, local)]);

    let mut request = spillable_request();
    request.placement_mobility = PlacementMobility::PinnedOrColocated;
    request.verdict_platforms = vec!["linux".into()];
    let refusal = place(&connections, &request).unwrap_err();
    assert!(refusal.contains("linux"), "{refusal}");
    assert!(refusal.contains("Nothing was run"), "{refusal}");
}

/// A mobile batch waits instead: a machine on a platform this verdict counts
/// from may still attach, and the requester's horizon already bounds how
/// long that hope lasts.
#[test]
fn mobile_work_waits_when_only_ungated_platforms_are_attached() {
    let (windows_id, mut windows) = fleet_entry("bglab-win", "windows", 0, &[]);
    measured(
        &mut windows,
        0.01,
        64 * 1024 * 1024 * 1024,
        900 * 1024 * 1024 * 1024,
    );
    let connections = HashMap::from([(windows_id, windows)]);

    let mut request = spillable_request();
    request.verdict_platforms = vec!["macos".into()];
    let draft = place(&connections, &request).unwrap();
    assert!(draft.selected.is_none(), "{draft:?}");
}

/// Naming one machine is different: the caller settled placement, there is
/// nothing for a measurement to decide, and the work runs where it was sent.
#[test]
fn naming_one_machine_runs_there_whether_or_not_it_can_be_measured() {
    let connections = HashMap::from([
        fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]),
        fleet_entry("bglab-ub", "linux", 0, &[]),
    ]);
    let mut request = spillable_request();
    request.executor = Some(ExecutorSelector {
        name: Some("bglab-ub".into()),
        ..ExecutorSelector::default()
    });
    let draft = place(&connections, &request).unwrap();
    let selection = chosen(&draft);
    assert_eq!(selection.executor_id, "bglab-ub");
    assert_eq!(
        selection.reason,
        PlacementReason::OnlyCandidate,
        "nothing was measured for, so the record must not claim the fleet was blind"
    );
}

/// A machine's last decline replaces what the runner reads of it now, a
/// structural reason is kept, and a decliner the rows no longer name is
/// still accounted for.
#[test]
fn the_journal_replaces_readings_but_not_structural_reasons() {
    let row = |id: &str, reason| PlacementRejection {
        unreadable_memory: Vec::new(),
        drain_blockers: Vec::new(),
        prediction: None,
        executor_name: id.into(),
        executor_id: id.into(),
        reason,
    };
    let declined = |id: &str| {
        row(
            id,
            PlacementRejectionReason::Declined {
                reason: DeclineReason::Disk,
                detail: Some("sweep in flight".into()),
                declined_at_unix_ms: NOW,
            },
        )
    };
    let rows = account_from_journal(
        vec![
            row(
                "stale",
                PlacementRejectionReason::ConnectionStale {
                    silence_ms: 120_000,
                    stale_after_ms: 30_000,
                },
            ),
            row("drained", PlacementRejectionReason::Drained),
        ],
        &[declined("stale"), declined("drained"), declined("gone")],
    );
    assert!(matches!(
        rows[0].reason,
        PlacementRejectionReason::Declined { .. }
    ));
    assert!(matches!(rows[1].reason, PlacementRejectionReason::Drained));
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[2].executor_id, "gone");
}

/// A checkout that already exists on one machine cannot be recreated
/// elsewhere from objects, so spilling it is not an option however idle the
/// alternative looks.
#[test]
fn an_existing_checkout_is_never_spilled() {
    let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]);
    measured(
        &mut local,
        0.95,
        8 * 1024 * 1024 * 1024,
        500 * 1024 * 1024 * 1024,
    );
    let (remote_id, mut remote) = fleet_entry("bglab-ub", "linux", 0, &[]);
    measured(
        &mut remote,
        0.01,
        48 * 1024 * 1024 * 1024,
        900 * 1024 * 1024 * 1024,
    );
    let connections = HashMap::from([(local_id, local), (remote_id, remote)]);

    let mut request = spillable_request();
    request.repository = RepositoryLocator::ExistingCheckout {
        project_id: "p".into(),
        repository_id: "repo".into(),
        absolute_path: "/repo".into(),
    };
    let draft = place(&connections, &request).unwrap();
    assert_eq!(chosen(&draft).executor_id, COLOCATED_EXECUTOR_ID);
    assert!(matches!(
        rejection_for(&draft, "bglab-ub"),
        PlacementRejectionReason::RepositoryNotTransferable { .. }
    ));
}

/// A machine is addressed by the name it advertises, normalized — which is
/// the same name `cairn://executors` publishes. Selecting by a label an
/// operator would actually type has to reach it, or the resource lists
/// addresses that placement cannot use.
#[test]
fn a_named_selector_reaches_the_machine_the_resource_publishes() {
    let mut connections = HashMap::from([
        fleet_entry("remote-a", "linux", 0, &[]),
        fleet_entry("remote-b", "linux", 0, &[]),
    ]);
    connections
        .get_mut("remote-a")
        .unwrap()
        .identity
        .display_name = "BGLab UB".into();
    connections
        .get_mut("remote-b")
        .unwrap()
        .identity
        .display_name = "bglab-mac".into();

    for typed in ["bglab-ub", "BGLab UB", "BGLAB_UB"] {
        let mut request = targeted_request("linux");
        request.executor = Some(ExecutorSelector {
            name: Some(typed.into()),
            ..ExecutorSelector::default()
        });
        assert_eq!(
            choose_executor(&connections, &request)
                .unwrap()
                .unwrap()
                .executor_id,
            "remote-a",
            "selector {typed}"
        );
    }

    // A toolchain the fleet does not advertise narrows a matching name to
    // nothing rather than falling back to the machine that shares its OS.
    let mut request = targeted_request("linux");
    request.executor = Some(ExecutorSelector {
        name: Some("bglab-ub".into()),
        required_toolchains: vec!["msvc".into()],
        ..ExecutorSelector::default()
    });
    assert!(choose_executor(&connections, &request).is_err());
}

/// A refusal that names only what was wanted leaves an agent guessing, which
/// is what opaque identities forced. It names the fleet too, from the same
/// cache the resource reads, and points at that resource.
#[test]
fn a_no_match_refusal_names_the_request_and_every_machine_that_exists() {
    let mut connections = HashMap::from([fleet_entry("remote-a", "linux", 0, &[])]);
    connections
        .get_mut("remote-a")
        .unwrap()
        .identity
        .display_name = "bglab-ub".into();
    let mut request = targeted_request("linux");
    request.executor = Some(ExecutorSelector {
        name: Some("bglab-win".into()),
        ..ExecutorSelector::default()
    });

    let refusal = choose_executor(&connections, &request).unwrap_err();

    assert!(refusal.contains("bglab-win"), "{refusal}");
    assert!(refusal.contains("bglab-ub"), "{refusal}");
    assert!(refusal.contains("linux"), "{refusal}");
    assert!(refusal.contains("rust"), "{refusal}");
    assert!(refusal.contains("cairn://executors"), "{refusal}");
}

/// The enrollment outlives every link to the machine. A record that existed
/// only while the machine was attached could not describe a machine that is
/// not, which is the state worth describing.
#[test]
fn an_enrolled_machine_is_projected_before_it_ever_attaches() {
    let fleet = Fleet::default();
    fleet.declare_enrolled_remote("bglab-ub", "bglab-ub", "linux", "x86_64");

    let unattached = fleet.unattached_enrolled_remotes();

    assert_eq!(unattached.len(), 1);
    assert_eq!(unattached[0].name, "bglab-ub");
    // Absent with nothing said about why: the runner does not reach for
    // this machine, so it has no attempt to report and no business
    // claiming one.
    assert!(matches!(
        unattached[0].link,
        RemoteLinkState::Absent {
            last_beat_unix_ms: None,
            ..
        }
    ));
    assert_eq!(unattached[0].last_attempt, None);
    assert_eq!(
        unattached[0].last_seen_unix_ms, None,
        "a machine that has never attached must not claim to have been seen"
    );
}

/// cairn/5510: a machine refused on protocol is stranded exactly while the
/// runner has resolved its target and holds nothing to repair it with, and
/// attaching ends the run of refusals.
#[tokio::test]
async fn a_refused_machine_is_stranded_only_while_the_runner_has_nothing_for_its_target() {
    const LINUX: &str = "x86_64-unknown-linux-gnu";
    let unpublished = || {
        Err(ArtifactUnavailable {
            reason:
                "no x86_64-unknown-linux-gnu executor artifact published for executor protocol v159"
                    .into(),
            newest_held_protocol: Some(157),
        })
    };
    let fleet = Fleet::default();
    fleet.declare_enrolled_remote("fedora", "fedora", "linux", "x86_64");
    fleet.declare_enrolled_remote("bgsrv-mac", "bgsrv-mac", "macos", "aarch64");
    assert_eq!(
        fleet
            .remote_executor_targets()
            .into_iter()
            .collect::<Vec<_>>(),
        ["aarch64-apple-darwin", LINUX],
        "a machine that never attached still has its target resolved"
    );
    let fedora = |fleet: &Fleet| {
        fleet
            .unattached_enrolled_remotes()
            .into_iter()
            .find(|remote| remote.name == "fedora")
            .unwrap()
            .stranded
    };

    assert_eq!(
        fleet.record_protocol_refusal("fedora", 159, 157),
        None,
        "an unresolved target claims nothing either way"
    );
    assert_eq!(fedora(&fleet), None);

    fleet.record_executor_artifact_build(LINUX, unpublished());
    let stranded = fleet
        .record_protocol_refusal("fedora", 159, 157)
        .expect("refused with nothing to serve is stranded");
    assert_eq!(
        (
            stranded.target.as_str(),
            stranded.expected_protocol,
            stranded.received_protocol,
            stranded.newest_held_protocol,
            stranded.refusals,
        ),
        (LINUX, 159, 157, Some(157), 2)
    );
    assert!(stranded.last_refused_unix_ms >= stranded.refused_since_unix_ms);
    assert_eq!(
        stranded.describe(),
        "stranded: this runner has no x86_64-unknown-linux-gnu executor for protocol 159 (newest held: v157); publish one or roll the app back"
    );
    assert_eq!(fedora(&fleet), Some(stranded));
    assert!(
        fleet
            .unattached_enrolled_remotes()
            .iter()
            .find(|remote| remote.name == "bgsrv-mac")
            .unwrap()
            .stranded
            .is_none(),
        "a machine never refused is not stranded"
    );
    assert_eq!(
        fleet.record_protocol_refusal("stranger", 159, 157),
        None,
        "an identity this runner never enrolled records nothing"
    );

    // Published: the same refusal is now a self-update that can succeed.
    fleet.record_executor_artifact_build(LINUX, Ok("build-159".into()));
    assert_eq!(fleet.record_protocol_refusal("fedora", 159, 157), None);
    assert_eq!(fedora(&fleet), None);

    fleet.record_executor_artifact_build(LINUX, unpublished());
    assert_eq!(fedora(&fleet).map(|stranded| stranded.refusals), Some(3));
    let (_, entry) = fleet_entry("fedora", "linux", 0, &[]);
    let generation = fleet
        .attach_advertised_executor(
            Arc::unwrap_or_clone(entry.advertisement),
            Default::default(),
            entry.sender,
            false,
            None,
            None,
        )
        .expect("fedora attaches");
    assert!(fleet.disconnect_advertised_executor("fedora", generation));
    assert_eq!(fedora(&fleet), None, "attaching ends the run of refusals");
}

/// The ceremony reports its own progress, and the distinction that survives
/// is the one a person acts on differently: a bootstrap that failed needs
/// someone, and a machine that was installed and has not dialled yet does
/// not.
#[test]
fn a_bootstrap_records_the_state_it_proved() {
    let fleet = Fleet::default();
    fleet.declare_enrolled_remote("bglab-ub", "bglab-ub", "linux", "x86_64");

    fleet.record_remote_attach_attempt(
        "bglab-ub",
        RemoteLinkState::Bootstrapping {
            phase: cairn_common::executor_protocol::BootstrapPhase::Installing,
        },
        RemoteAttachAttempt::bootstrap_failure(4_000, "installing the executor binary"),
    );
    let running = fleet.unattached_enrolled_remotes();
    assert!(matches!(
        running[0].link,
        RemoteLinkState::Bootstrapping { .. }
    ));

    fleet.record_remote_attach_attempt(
        "bglab-ub",
        RemoteLinkState::BootstrapFailed {
            cause: "executor protocol v28 has no published artifact".to_string(),
        },
        RemoteAttachAttempt::bootstrap_failure(
            5_000,
            "executor protocol v28 has no published artifact",
        ),
    );
    let failed = fleet.unattached_enrolled_remotes();
    let attempt = failed[0].last_attempt.as_ref().unwrap();
    assert!(matches!(
        failed[0].link,
        RemoteLinkState::BootstrapFailed { .. }
    ));
    assert_eq!(attempt.attempted_at_unix_ms, 5_000);
    assert!(attempt.summary.contains("no published artifact"));

    // Installed and not yet dialled is NOT a failure, and must not read as
    // one: nothing is retried because nothing on this side is doing the
    // connecting.
    fleet.record_remote_attach_attempt(
        "bglab-ub",
        RemoteLinkState::NeverConnected {
            since_bootstrap_unix_ms: 6_000,
        },
        RemoteAttachAttempt::bootstrap_failure(6_000, "installed; waiting for first dial"),
    );
    let waiting = fleet.unattached_enrolled_remotes();
    assert!(matches!(
        waiting[0].link,
        RemoteLinkState::NeverConnected { .. }
    ));
}

/// A machine that never dials is not a failure, and the wait for it is
/// bounded.
///
/// The ceremony genuinely finished; what has not happened is on the other
/// side of a link this runner does not own and cannot provoke. Waiting
/// forever would hold an enrollment open on a machine that may simply be
/// slow to boot, and reporting a failure would send someone looking for a
/// break that is not there.
#[tokio::test(start_paused = true)]
async fn waiting_for_a_machine_that_never_dials_is_bounded_and_not_a_failure() {
    let fleet = Fleet::default();
    fleet.declare_enrolled_remote("bglab-ub", "bglab-ub", "linux", "x86_64");

    let arrived = fleet
        .wait_for_enrolled_remote("bglab-ub", Duration::from_millis(60_000))
        .await;

    assert!(!arrived, "nothing dialled, so the wait must report that");
    // And the enrollment is untouched by the wait: only the machine's own
    // connection moves it to connected.
    let enrolled = fleet.unattached_enrolled_remotes();
    assert_eq!(enrolled.len(), 1);
    assert!(!enrolled[0].link.is_connected());
}

/// The first-dial wait accepts any generation that attaches while it
/// runs, not only one present when it began. The specimen's machine died
/// at generation 1 and came back as generation 2; that is a dial.
#[tokio::test]
async fn a_later_generation_attaching_during_the_first_dial_wait_ends_it() {
    let fleet = std::sync::Arc::new(Fleet::default());
    fleet.declare_enrolled_remote("sp1", "sp1", "linux", "aarch64");
    let (_, first) = fleet_entry("sp1", "linux", 0, &[]);
    let generation = fleet
        .attach_advertised_executor(
            Arc::unwrap_or_clone(first.advertisement),
            Default::default(),
            first.sender,
            false,
            None,
            None,
        )
        .expect("generation 1 attaches");
    assert!(fleet.disconnect_advertised_executor("sp1", generation));
    assert!(
        !fleet.wait_for_enrolled_remote("sp1", Duration::ZERO).await,
        "generation 1 is gone"
    );

    let waiting = tokio::spawn({
        let fleet = fleet.clone();
        async move {
            fleet
                .wait_for_enrolled_remote("sp1", Duration::from_secs(2))
                .await
        }
    });
    // Long enough for the waiter to have checked, found nothing, and
    // registered for the next attach, so the dial below is one it hears
    // rather than one it finds already present.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!waiting.is_finished(), "no generation is attached yet");
    let (_, second) = fleet_entry("sp1", "linux", 0, &[]);
    let later = fleet
        .attach_advertised_executor(
            Arc::unwrap_or_clone(second.advertisement),
            Default::default(),
            second.sender,
            false,
            None,
            None,
        )
        .expect("generation 2 attaches");

    assert!(later > generation);
    assert!(
        tokio::time::timeout(Duration::from_secs(10), waiting)
            .await
            .expect("the wait returns")
            .unwrap(),
        "the later generation is the dial the wait was for"
    );
}

/// The defect this whole seam exists for: a drain must be settable on a
/// machine that is answering nothing, because that is when an operator
/// reaches for it. The write records the refusal and says plainly that the
/// machine has not been told.
#[tokio::test]
async fn a_machine_that_is_not_attached_can_still_be_drained() {
    let fleet = Fleet::default();
    fleet.declare_enrolled_remote("bglab-sp1", "bglab-sp1", "linux", "x86_64");

    let application = fleet.set_executor_drain_mode("bglab-sp1", true).await;

    assert!(matches!(
        application,
        DrainApplication::HeldWithoutMachine { .. }
    ));
    assert!(fleet.is_drained("bglab-sp1"));
    assert!(
        fleet.unattached_enrolled_remotes()[0].drained,
        "the fleet surface has to show a drain the runner is holding, or an operator cannot tell it took"
    );
}

/// The durability half. A drained machine that comes back — a power cycle,
/// a supervisor respawn, a new generation — is still drained, and is told
/// so as part of attaching. Before this, the drain evaporated on reconnect
/// and the fleet re-dogpiled the machine it had just crushed.
#[tokio::test]
async fn a_drain_outlives_the_connection_it_was_set_on() {
    let fleet = Fleet::default();
    let (first_sender, _first) = mpsc::unbounded_channel();
    let first_generation = fleet.attach_executor(first_sender);

    fleet
        .set_executor_drain_mode(COLOCATED_EXECUTOR_ID, true)
        .await;
    fleet.disconnect_advertised_executor(COLOCATED_EXECUTOR_ID, first_generation);

    let (sender, mut executor) = mpsc::unbounded_channel();
    let second_generation = fleet.attach_executor(sender);
    assert!(second_generation > first_generation);
    assert_eq!(drain_told(&mut executor), Some(true));
    assert!(fleet.is_drained(COLOCATED_EXECUTOR_ID));
}

/// The other direction, and the one that inverts the fleet if it is missed.
///
/// Drain mode is executor-PROCESS state, and a process outlives a link
/// bounce. Clearing a drain while the machine is detached — the path this
/// change exists to allow — has to reach that process when its link comes
/// back, or the executor keeps refusing work the runner has started placing
/// again: each side correct about itself, opposite about the fleet.
#[tokio::test]
async fn clearing_a_drain_while_detached_reaches_the_same_process_on_reconnect() {
    let fleet = Fleet::default();
    // Drained while unreachable, which is the path this change allows.
    fleet
        .set_executor_drain_mode(COLOCATED_EXECUTOR_ID, true)
        .await;

    // Its process was running the whole time; attaching is where it learns.
    let (first_sender, mut first) = mpsc::unbounded_channel();
    let first_generation = fleet.attach_executor(first_sender);
    assert_eq!(drain_told(&mut first), Some(true));

    // The link drops. The process does not.
    fleet.disconnect_advertised_executor(COLOCATED_EXECUTOR_ID, first_generation);
    fleet
        .set_executor_drain_mode(COLOCATED_EXECUTOR_ID, false)
        .await;
    assert!(!fleet.is_drained(COLOCATED_EXECUTOR_ID));

    let (sender, mut executor) = mpsc::unbounded_channel();
    fleet.attach_executor(sender);

    assert_eq!(
        drain_told(&mut executor),
        Some(false),
        "the returning process still holds the drain it was last told about, so the clear has to be stated to it"
    );
    assert!(
        !fleet
            .lock_connections()
            .get(COLOCATED_EXECUTOR_ID)
            .unwrap()
            .drained
    );
}

/// What the runner told an executor about draining on this connection, if
/// anything. Drains the queue rather than reading one message, because an
/// attach also carries whatever else the runner has to say.
fn drain_told(executor: &mut mpsc::UnboundedReceiver<ExecutorMessage>) -> Option<bool> {
    let mut told = None;
    while let Ok(message) = executor.try_recv() {
        if let ExecutorMessage::DrainModeRequest { enabled, .. } = message {
            told = Some(enabled);
        }
    }
    told
}

/// Placement is the runner's own decision, so a drained machine is passed
/// over here rather than by asking the machine to refuse the admission. A
/// machine too wedged to answer is exactly the one whose refusal cannot be
/// relied on.
#[test]
fn placement_passes_over_a_drained_machine_without_asking_it() {
    let mut connections = HashMap::from([
        fleet_entry(COLOCATED_EXECUTOR_ID, "macos", 0, &[]),
        fleet_entry("bglab-sp1", "linux", 0, &[]),
    ]);
    connections
        .get_mut(COLOCATED_EXECUTOR_ID)
        .unwrap()
        .colocated = true;
    connections.get_mut("bglab-sp1").unwrap().drained = true;
    let mut request = targeted_request("linux");
    request.executor = Some(ExecutorSelector {
        name: Some("bglab-sp1".into()),
        ..ExecutorSelector::default()
    });

    let refusal = place(&connections, &request)
        .expect_err("a fleet whose only matching machine is drained refuses");
    assert!(refusal.contains("bglab-sp1"), "{refusal}");
    assert!(refusal.contains("drained"), "{refusal}");
    assert!(refusal.contains("Blocking records/fences"), "{refusal}");

    connections.get_mut("bglab-sp1").unwrap().drained = false;
    assert_eq!(
        place(&connections, &request)
            .unwrap()
            .selected
            .expect("clearing the drain re-admits it")
            .0
            .executor_id,
        "bglab-sp1"
    );
}

fn model_resident(phase: &str, held_gib: u64) -> MachineModelResident {
    MachineModelResident {
        model: "deepseek-ai/DeepSeek-V4-Flash".into(),
        deployment: "dsv4f-tp2".into(),
        phase: phase.into(),
        held_bytes: Some(held_gib << 30),
        unified: true,
        pinned: false,
        leased: false,
        evictable_at_unix_ms: None,
    }
}

fn demand(gib: u64, source: ResourceReservationSource) -> ResourceReservation {
    ResourceReservation {
        memory_bytes: gib << 30,
        source,
        ..ResourceReservation::default()
    }
}

/// A machine whose room a loading model has not taken yet refuses
/// measured work that would need it, naming the model, and the refusal is
/// waited on. Work whose memory was never measured is refused that room
/// too (it fails closed), marked so it never evicts. A residency pin is
/// exempt as from a drain, a healthy model leaves its machine's measured
/// room to batches, and a discrete VRAM model never refuses one.
#[test]
fn a_loading_model_s_room_refuses_measured_work_and_is_waited_on() {
    let (_, mut sp1) = fleet_entry("bginf-sp1", "linux", 0, &[]);
    measured(&mut sp1, 0.1, 110 << 30, 500 << 30);
    sp1.model_residents = vec![model_resident("reserving", 100)];
    let learned = demand(20, ResourceReservationSource::Learned);
    let reason = model_room_rejection(&sp1, Some(&learned), false)
        .expect("10 GiB is left beside the model's 100");
    assert!(waits_on_machine(&reason));
    assert_eq!(placement_rejection_signature(&reason), "modelResident");
    assert!(
        reason.describe().starts_with(
            "room is held by deepseek-ai/DeepSeek-V4-Flash (reserving, 100.0 GiB); it is loading"
        ),
        "{}",
        reason.describe()
    );
    assert_eq!(
        model_room_rejection(&sp1, Some(&learned), true),
        None,
        "pinned here: work whose environment lives here finishes here"
    );
    for unmeasured in [
        Some(demand(20, ResourceReservationSource::Unmeasured)),
        Some(demand(0, ResourceReservationSource::Learned)),
        None,
    ] {
        let reason = model_room_rejection(&sp1, unmeasured.as_ref(), false)
            .expect("nothing shows unmeasured work fits beside the reserved room");
        assert!(waits_on_machine(&reason));
        assert!(
            matches!(
                reason,
                PlacementRejectionReason::ModelResident {
                    demand_unmeasured: true,
                    ..
                }
            ),
            "{reason:?}"
        );
        assert!(
            reason.describe().starts_with(
                "room is reserved for deepseek-ai/DeepSeek-V4-Flash (reserving, 100.0 GiB); this work's memory has never been measured"
            ),
            "{}",
            reason.describe()
        );
    }
    assert!(model_room_rejection(
        &sp1,
        Some(&demand(8, ResourceReservationSource::Declared)),
        false
    )
    .is_none());

    sp1.model_residents = vec![model_resident("healthy", 100)];
    assert_eq!(
        model_room_rejection(&sp1, Some(&learned), false),
        None,
        "its memory is already out of the 110 GiB measured free"
    );
    sp1.model_residents = vec![MachineModelResident {
        unified: false,
        ..model_resident("reserving", 20)
    }];
    assert_eq!(model_room_rejection(&sp1, Some(&learned), false), None);
}

/// Residents are stamped by public name on live connections and on
/// attach, and replaced whole.
#[test]
fn model_residents_stamp_live_connections_by_name() {
    let fleet = Fleet::default();
    let (_name, entry) = fleet_entry("bginf-sp2", "linux", 0, &[]);
    fleet.lock_connections().insert("bginf-sp2".into(), entry);
    let resident = model_resident("healthy", 98);
    fleet.set_model_residents(HashMap::from([(
        "bginf-sp2".to_string(),
        vec![resident.clone()],
    )]));
    assert_eq!(
        fleet.lock_connections()["bginf-sp2"].model_residents,
        vec![resident.clone()]
    );
    assert_eq!(fleet.model_residents_named("bginf-sp2"), vec![resident]);
    fleet.set_model_residents(HashMap::new());
    assert!(fleet.lock_connections()["bginf-sp2"]
        .model_residents
        .is_empty());
}

fn busy_home_and_idle_remote(role: MachineRole) -> HashMap<String, ExecutorConnectionState> {
    let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]);
    let (remote_id, mut remote) = fleet_entry("sp1", "linux", 0, &[]);
    measured(&mut local, 0.9, 8_000_000_000, 8_000_000_000);
    measured(&mut remote, 0.05, 64_000_000_000, 800_000_000_000);
    remote.machine_role = role;
    HashMap::from([(local_id, local), (remote_id, remote)])
}

/// `run` with `executor:{name:"bgsrv-ub2"}` on a services box is refused
/// before anything runs, in the role's words.
#[test]
fn a_batch_pinned_to_a_services_only_machine_is_refused_naming_the_role() {
    let connections = busy_home_and_idle_remote(MachineRole::ServicesOnly);
    let mut request = targeted_request("linux");
    request.placement_work_class = PlacementWorkClass::AgentSessions;
    request.executor = Some(ExecutorSelector {
        name: Some("sp1".into()),
        ..ExecutorSelector::default()
    });
    assert_eq!(
        place(&connections, &request).expect_err("a batch is refused"),
        "sp1 runs services only; this request is agentSessions. Its role is set by an operator on cairn://executors/sp1."
    );
}

fn isolation(verdict: PublishIsolationVerdict, age_ms: u64) -> PublishIsolationEvidence {
    PublishIsolationEvidence {
        verdict,
        measured_at_unix_ms: NOW - age_ms,
        lines: Vec::new(),
    }
}

fn publication_leg(publisher_required: bool) -> CellRequest {
    let mut request = spillable_request();
    request.placement_work_class = PlacementWorkClass::ReviewChecks;
    request.publication = Some(PublicationCustody { publisher_required });
    request
}

/// A secret-bearing leg goes only to a publisher whose isolation measured
/// PASS recently. Every other answer is a refusal naming why, a pin does
/// not lift it, and unmeasured is never a pass.
#[test]
fn a_secret_bearing_leg_goes_only_to_a_freshly_measured_publisher() {
    let stale = PUBLISH_ISOLATION_STALE_AFTER_MS + 60_000;
    let cases: [(MachineRole, Option<PublishIsolationEvidence>, Option<&str>); 7] = [
        (MachineRole::General, None, Some("role general")),
        (MachineRole::BuildsOnly, None, Some("role buildsOnly")),
        (
            MachineRole::PublishOnly,
            None,
            Some("isolation unmeasured: executor not started as a publisher"),
        ),
        (
            MachineRole::PublishOnly,
            Some(isolation(
                PublishIsolationVerdict::Unmeasured {
                    reason: "probe pending".into(),
                },
                0,
            )),
            Some("isolation unmeasured: probe pending"),
        ),
        (
            MachineRole::PublishOnly,
            Some(isolation(
                PublishIsolationVerdict::Fail {
                    reason: "home is open".into(),
                },
                0,
            )),
            Some("isolation FAIL: home is open"),
        ),
        (
            MachineRole::PublishOnly,
            Some(isolation(PublishIsolationVerdict::Pass, stale)),
            Some("stale"),
        ),
        (
            MachineRole::PublishOnly,
            Some(isolation(PublishIsolationVerdict::Pass, 4 * 60_000)),
            None,
        ),
    ];
    for (role, evidence, refused) in cases {
        let mut connections = busy_home_and_idle_remote(role);
        Arc::make_mut(&mut connections.get_mut("sp1").unwrap().advertisement)
            .capabilities
            .publish_isolation = evidence.clone();
        let mut pinned = publication_leg(true);
        pinned.pinned_executor_id = Some("sp1".into());
        for request in [publication_leg(true), pinned] {
            let reason =
                candidate_rejection(&connections["sp1"], PlacementScope::of(&request), NOW);
            match refused {
                None => assert_eq!(reason, None, "{role:?} {evidence:?}"),
                Some(why) => {
                    let Some(PlacementRejectionReason::NotPublishEligible { why: got }) = &reason
                    else {
                        panic!("{role:?} {evidence:?}: {reason:?}");
                    };
                    assert!(got.contains(why), "{got}");
                    assert!(reason
                        .as_ref()
                        .unwrap()
                        .describe()
                        .contains("release secrets go only to publishOnly executors whose isolation measured PASS"));
                }
            }
        }
    }
}

/// An executor started as a publisher (it advertises a verdict) under any
/// other role is refused everything, a pin included: its account holds
/// the secret.
#[test]
fn a_publisher_enrolled_under_another_role_is_refused_all_work() {
    let mut connections = busy_home_and_idle_remote(MachineRole::General);
    Arc::make_mut(&mut connections.get_mut("sp1").unwrap().advertisement)
        .capabilities
        .publish_isolation = Some(isolation(PublishIsolationVerdict::Pass, 0));
    let mut pinned = spillable_request();
    pinned.pinned_executor_id = Some("sp1".into());
    for request in [
        spillable_request(),
        publication_leg(false),
        publication_leg(true),
        pinned,
    ] {
        assert!(matches!(
            candidate_rejection(&connections["sp1"], PlacementScope::of(&request), NOW),
            Some(PlacementRejectionReason::NotPublishEligible { why })
                if why.starts_with("started as a publisher but enrolled as general")
        ));
    }
}

/// A pin by name is refused before anything runs, naming the machine, its
/// role, the class, and who can change it (cairn/4479).
#[test]
fn a_pin_by_name_onto_an_excluding_role_is_refused_naming_role_and_class() {
    let mut connections = busy_home_and_idle_remote(MachineRole::InferenceOnly);
    let mut request = targeted_request("linux");
    request.placement_work_class = PlacementWorkClass::AgentSessions;
    request.executor = Some(ExecutorSelector {
        name: Some("sp1".into()),
        ..ExecutorSelector::default()
    });
    let refusal = place(&connections, &request).expect_err("an excluded pin is refused");
    assert_eq!(
        refusal,
        "sp1 runs inference only; this request is agentSessions. Its role is set by an operator on cairn://executors/sp1."
    );

    // A permanent role outranks a drain an operator could clear.
    connections.get_mut("sp1").unwrap().drained = true;
    let refusal = place(&connections, &request).expect_err("still refused");
    assert!(refusal.contains("inference only"), "{refusal}");
    assert!(!refusal.contains("drained"), "{refusal}");
}

/// Work already resident on a machine when its role changed may finish, as
/// it may under a drain.
#[test]
fn a_residency_pin_crosses_a_role_that_changed_under_it() {
    let connections = busy_home_and_idle_remote(MachineRole::InferenceOnly);
    let mut request = targeted_request("linux");
    request.executor = None;
    request.pinned_executor_id = Some("sp1".into());
    let selection = place(&connections, &request)
        .expect("resident work crosses the role gate")
        .selected
        .expect("its home is the only destination")
        .0;
    assert_eq!(selection.executor_id, "sp1");
}

/// The colocated machine is bound by its role like any other, and work
/// that cannot leave it is told so at once rather than waiting out its
/// horizon on a fact only an operator changes.
#[test]
fn the_colocated_machine_is_not_exempt_from_its_role() {
    let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]);
    measured(&mut local, 0.1, 8_000_000_000, 8_000_000_000);
    local.machine_role = MachineRole::InferenceOnly;
    let connections = HashMap::from([(local_id, local)]);
    let mut request = spillable_request();
    request.placement_mobility = PlacementMobility::PinnedOrColocated;
    let refusal = place(&connections, &request).expect_err("immobile work is refused");
    assert!(
        refusal.contains("runs inference only; this request is agentSessions"),
        "{refusal}"
    );
    assert!(refusal.contains("Nothing was run"), "{refusal}");

    let mobile = spillable_request();
    let draft = place(&connections, &mobile).expect("mobile work waits for a machine");
    assert!(draft.selected.is_none());
    assert!(matches!(
        rejection_for(&draft, COLOCATED_EXECUTOR_ID),
        PlacementRejectionReason::RoleExcluded { .. }
    ));
}

/// An attach that is already under way when a role is adopted must come
/// out with the adopted role. The table lock is held here the way an
/// adoption holds it, so the attach parks at its insert; the role changes
/// while it waits. An attach that read the role before taking the lock
/// would insert the role it read and keep it until the next adoption.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_attach_racing_a_role_adoption_comes_out_with_the_adopted_role() {
    let fleet = Fleet::default();
    let table = fleet.lock_connections();
    let attaching = {
        let fleet = fleet.clone();
        tokio::task::spawn_blocking(move || {
            let (sender, receiver) = mpsc::unbounded_channel();
            fleet.attach_executor(sender);
            receiver
        })
    };
    // Long enough for the attach to do everything ahead of the table lock.
    std::thread::sleep(Duration::from_millis(200));
    // The adoption's write, made under the table lock as it makes it.
    fleet.machine_roles.lock().unwrap().insert(
        COLOCATED_EXECUTOR_ID.to_string(),
        MachineRole::InferenceOnly,
    );
    drop(table);
    let _receiver = attaching.await.unwrap();
    assert_eq!(
        fleet.live_machine_role_for_test(COLOCATED_EXECUTOR_ID),
        Some(MachineRole::InferenceOnly)
    );
}

/// Attach and hydrate both stamp the entry placement reads, in either
/// order, from the one cache.
#[tokio::test]
async fn attach_and_hydrate_stamp_the_machine_role_in_either_order() {
    let attached_first = Fleet::default();
    let (sender, _executor) = mpsc::unbounded_channel();
    attached_first.attach_executor(sender);
    assert_eq!(
        attached_first.live_machine_role_for_test(COLOCATED_EXECUTOR_ID),
        Some(MachineRole::General)
    );
    attached_first
        .hydrate_machine_roles([(COLOCATED_EXECUTOR_ID.to_string(), MachineRole::BuildsOnly)]);
    assert_eq!(
        attached_first.live_machine_role_for_test(COLOCATED_EXECUTOR_ID),
        Some(MachineRole::BuildsOnly)
    );
    attached_first.hydrate_machine_roles(Vec::new());
    assert_eq!(
        attached_first.live_machine_role_for_test(COLOCATED_EXECUTOR_ID),
        Some(MachineRole::General),
        "adopting a config with no role clears the stamp"
    );

    let hydrated_first = Fleet::default();
    hydrated_first.hydrate_machine_roles([(
        COLOCATED_EXECUTOR_ID.to_string(),
        MachineRole::InferenceOnly,
    )]);
    let (sender, _executor) = mpsc::unbounded_channel();
    hydrated_first.attach_executor(sender);
    assert_eq!(
        hydrated_first.live_machine_role_for_test(COLOCATED_EXECUTOR_ID),
        Some(MachineRole::InferenceOnly)
    );
}

#[test]
fn a_drain_refuses_new_work_even_when_pinned_to_an_existing_home() {
    let (executor_id, mut resident_home) = fleet_entry("bglab-sp1", "linux", 0, &[]);
    resident_home.drained = true;
    let connections = HashMap::from([(executor_id, resident_home)]);
    let mut request = targeted_request("linux");
    request.pinned_executor_id = Some("bglab-sp1".into());
    let refusal = place(&connections, &request).expect_err("pins do not bypass drains");
    assert!(refusal.contains("drained"), "{refusal}");
}

/// Adopting durable state at startup must not depend on winning a race with
/// the colocated executor's attach, which happens inside the same boot.
#[tokio::test]
async fn hydrating_a_drain_reaches_a_machine_that_attached_first() {
    let fleet = Fleet::default();
    let (sender, mut executor) = mpsc::unbounded_channel();
    fleet.attach_executor(sender);
    assert_eq!(
        drain_told(&mut executor),
        Some(false),
        "attaching states this machine's drain either way"
    );

    fleet.hydrate_drained([COLOCATED_EXECUTOR_ID.to_string()]);

    assert_eq!(
        drain_told(&mut executor),
        Some(true),
        "a machine already up is told about the drain it is under"
    );
    assert!(
        fleet
            .lock_connections()
            .get(COLOCATED_EXECUTOR_ID)
            .unwrap()
            .drained
    );

    // And an adoption holding no drain says so, because a runner restart's
    // settings are not evidence about what a surviving executor process is
    // still doing to its own admission.
    fleet.hydrate_drained(Vec::<String>::new());
    assert_eq!(drain_told(&mut executor), Some(false));
}

#[test]
fn removing_a_remote_generation_leaves_the_colocated_generation_stable() {
    let fleet = Fleet::default();
    let (remote_id, remote) = fleet_entry("bglab-ub", "linux", 0, &[]);
    let (local_id, local) = fleet_entry(COLOCATED_EXECUTOR_ID, "macos", 0, &[]);
    fleet.lock_connections().insert(remote_id, remote);
    fleet.lock_connections().insert(local_id, local);

    assert_eq!(
        fleet.managed_link_silence_ms("bglab-ub", 1, NOW + 10),
        Some(10)
    );
    assert!(fleet.disconnect_advertised_executor("bglab-ub", 1));
    assert_eq!(fleet.managed_link_silence_ms("bglab-ub", 1, NOW + 10), None);
    assert_eq!(fleet.executor_generation(), Some(1));
}

#[test]
fn remote_connection_history_is_bounded_and_newest_first() {
    let fleet = Fleet::default();
    fleet.declare_enrolled_remote("bglab-ub", "bglab-ub", "linux", "x86_64");

    for generation in 1..=REMOTE_CONNECTION_TIMELINE_LIMIT as u64 + 3 {
        fleet.record_remote_connection_transition(
            "bglab-ub",
            RemoteConnectionTransition {
                occurred_at_unix_ms: generation,
                phase: RemoteConnectionPhase::ProtocolReady,
                generation: Some(generation),
                reason: None,
                remote_process_exit_status: None,
                last_stderr: None,
                memory_at_generation_end: None,
            },
        );
    }

    let remote = &fleet.unattached_enrolled_remotes()[0];
    assert_eq!(
        remote.connection_timeline.len(),
        REMOTE_CONNECTION_TIMELINE_LIMIT
    );
    assert_eq!(remote.connection_timeline[0].generation, Some(35));
    assert_eq!(
        remote.connection_timeline.last().unwrap().generation,
        Some(4)
    );
}

/// The reading a kill would otherwise destroy.
///
/// An executor's memory telemetry lives inside the executor. When the
/// kernel killed a Linux machine here for its size, the size it died at
/// appeared nowhere afterwards — the process holding the measurement was
/// the process that died, and the one that replaced it reported a fresh
/// start from nothing. Retaining the last sample on the runner is what
/// makes the end of a generation describable at all, and it is the reading
/// the whole investigation turns on: resident size says whether memory was
/// the cause, and the allocator split says where the fix belongs.
#[test]
fn a_generation_that_died_still_reports_the_size_it_reached() {
    let fleet = Fleet::default();
    fleet.declare_enrolled_remote("bglab-ub", "bglab-ub", "linux", "x86_64");
    let (remote_id, remote) = fleet_entry("bglab-ub", "linux", 0, &[]);
    fleet.lock_connections().insert(remote_id, remote);

    let mut health = ExecutorSubstrateReport::default();
    health.machine.process.resident_bytes = Measurement::measured(1, 10_200_547_328);
    health.machine.process.allocator = Measurement::measured(
        1,
        AllocatorState {
            in_use_bytes: 43_000_000,
            free_retained_bytes: 10_100_000_000,
            arena_bytes: 10_150_000_000,
            mmapped_bytes: 1_048_576,
            mmap_chunk_count: 1,
        },
    );
    // The return value reports whether the public cell snapshot changed,
    // which is a different question from whether the health was taken. The
    // assertion that matters is downstream, on the transition.
    fleet.set_executor_snapshot("bglab-ub", 1, FleetSnapshot::default(), health);

    // A step on the way to a generation is not its end, and pinning a size
    // to it would date the reading to before anything was measured.
    fleet.record_remote_connection_transition(
        "bglab-ub",
        transition_for(RemoteConnectionPhase::ProtocolReady, 1),
    );
    // Then the machine goes away the way this one goes away.
    assert!(fleet.disconnect_advertised_executor("bglab-ub", 1));
    fleet.record_remote_connection_transition(
        "bglab-ub",
        transition_for(RemoteConnectionPhase::RemoteProcessExited, 1),
    );
    // And a generation that never got as far as reporting has no size to
    // claim, which must read as absent rather than as a healthy zero.
    fleet.record_remote_connection_transition(
        "bglab-ub",
        transition_for(RemoteConnectionPhase::RemoteProcessExited, 2),
    );

    let remote = &fleet.unattached_enrolled_remotes()[0];
    // Newest first.
    let [never_reported, ended, ready] = &remote.connection_timeline[..] else {
        panic!("three transitions: {:?}", remote.connection_timeline);
    };

    let memory = ended
        .memory_at_generation_end
        .as_ref()
        .expect("a generation that reported and then died carries its last size");
    assert_eq!(
        memory.process.resident_bytes.value().copied(),
        Some(10_200_547_328)
    );
    assert_eq!(
        memory
            .process
            .allocator
            .value()
            .map(|state| state.free_retained_bytes),
        Some(10_100_000_000),
        "the half of the reading that says the bytes had no owner"
    );

    assert!(
        never_reported.memory_at_generation_end.is_none(),
        "a generation that never reported must not inherit another's size"
    );
    assert!(
        ready.memory_at_generation_end.is_none(),
        "a generation's size belongs to the moment it ended, not to the \
             moment it began"
    );
}

fn transition_for(phase: RemoteConnectionPhase, generation: u64) -> RemoteConnectionTransition {
    RemoteConnectionTransition {
        occurred_at_unix_ms: unix_time_ms(),
        phase,
        generation: Some(generation),
        reason: None,
        remote_process_exit_status: None,
        last_stderr: None,
        memory_at_generation_end: None,
    }
}

/// An attached machine is described in full by the executor projections, so
/// listing it here too would invite two descriptions of one machine to
/// disagree.
#[test]
fn an_attached_machine_is_absent_from_the_unattached_projection() {
    let fleet = Fleet::default();
    fleet.declare_enrolled_remote("bglab-ub", "bglab-ub", "linux", "x86_64");
    let (id, entry) = fleet_entry("bglab-ub", "linux", 0, &[]);
    fleet.lock_connections().insert(id, entry);

    assert!(fleet.unattached_enrolled_remotes().is_empty());
}

#[test]
fn listener_rotation_gate_covers_every_non_ceremony_enrollment() {
    let fleet = Fleet::default();
    for id in ["absent", "bootstrapping", "bootstrap-failed"] {
        fleet.declare_enrolled_remote(id, id, "linux", "x86_64");
    }
    fleet.record_remote_attach_attempt(
        "bootstrapping",
        RemoteLinkState::Bootstrapping {
            phase: cairn_common::executor_protocol::BootstrapPhase::Installing,
        },
        RemoteAttachAttempt::bootstrap_failure(1, "installing"),
    );
    fleet.record_remote_attach_attempt(
        "bootstrap-failed",
        RemoteLinkState::BootstrapFailed {
            cause: "failed".into(),
        },
        RemoteAttachAttempt::bootstrap_failure(2, "failed"),
    );

    assert_eq!(fleet.listener_rotation_gate_executor_ids(), vec!["absent"]);
    fleet.forget_enrolled_remote("absent");
    assert!(fleet.listener_rotation_gate_executor_ids().is_empty());
}

/// A machine that dialed in holds the current pins and is about to be sent
/// `ListenerUpdated`, so cutting over without its acknowledgement strands
/// it whether or not a declaration for it has landed yet. The colocated
/// executor is the one connection that cannot be stranded: it pins no
/// listener certificate at all.
#[test]
fn listener_rotation_gate_covers_connected_machines_that_were_never_declared() {
    let fleet = Fleet::default();
    fleet.declare_enrolled_remote("declared", "Declared", "linux", "x86_64");
    for id in ["declared", "dialed-in", COLOCATED_EXECUTOR_ID] {
        let (executor_id, entry) = fleet_entry(id, "linux", 0, &[]);
        fleet.lock_connections().insert(executor_id, entry);
    }

    // "declared" is both enrolled and connected, and is gated once.
    assert_eq!(
        fleet.listener_rotation_gate_executor_ids(),
        vec!["declared", "dialed-in"]
    );
}

#[tokio::test]
async fn listener_publication_notifies_live_subscribers() {
    let fleet = Fleet::default();
    let mut updates = fleet.subscribe_executor_listener();
    let advertisement = ExecutorListenerAdvertisement {
        endpoints: vec!["wss://runner:3850/api/executor".into()],
        cert_sha256: vec!["new".into(), "old".into()],
    };
    fleet.publish_executor_listener(advertisement.clone());

    updates.changed().await.expect("publication");
    assert_eq!(*updates.borrow_and_update(), Some(advertisement.clone()));
    assert_eq!(fleet.executor_listener(), Some(advertisement));
}

/// A machine that closed its own link said why, and the absent link says
/// that rather than reading as a network fault.
#[test]
fn a_link_the_executor_closed_names_the_executors_reason() {
    let fleet = Fleet::default();
    fleet.declare_enrolled_remote("bglab-ub", "bglab-ub", "linux", "x86_64");
    let (executor_id, entry) = fleet_entry("bglab-ub", "linux", 0, &[]);
    let generation = entry.generation;
    fleet.lock_connections().insert(executor_id, entry);

    assert!(fleet.disconnect_advertised_executor_with_origin(
        "bglab-ub",
        generation,
        ExecutorDisconnectOrigin::ExecutorInitiated("executor stopping: SIGTERM received".into()),
    ));

    let unattached = fleet.unattached_enrolled_remotes();
    assert_eq!(unattached.len(), 1);
    assert!(
        matches!(
            &unattached[0].link,
            RemoteLinkState::Absent { last_reason, .. }
                if last_reason == "the executor closed this link: executor stopping: SIGTERM received"
        ),
        "{:?}",
        unattached[0].link
    );
}

/// Removing a machine takes it out of the fleet rather than leaving behind
/// one that fails to attach forever.
#[test]
fn forgetting_an_enrollment_removes_it_rather_than_failing_it() {
    let fleet = Fleet::default();
    fleet.declare_enrolled_remote("bglab-ub", "bglab-ub", "linux", "x86_64");
    fleet.record_remote_attach_attempt(
        "bglab-ub",
        RemoteLinkState::Absent {
            last_beat_unix_ms: None,
            last_reason: String::new(),
        },
        RemoteAttachAttempt::bootstrap_failure(4_000, "no route to host"),
    );

    fleet.forget_enrolled_remote("bglab-ub");

    assert!(fleet.unattached_enrolled_remotes().is_empty());
    // A machine nobody is enrolled with cannot accumulate attempts either.
    fleet.record_remote_attach_attempt(
        "bglab-ub",
        RemoteLinkState::BootstrapFailed {
            cause: String::new(),
        },
        RemoteAttachAttempt::bootstrap_failure(6_000, "stale supervisor"),
    );
    assert!(fleet.unattached_enrolled_remotes().is_empty());
}

/// An empty fleet says so rather than printing an empty list, and never
/// leaks an internal identity in place of a name.
#[test]
fn a_refusal_against_an_empty_fleet_says_nothing_is_attached() {
    let mut request = targeted_request("linux");
    request.executor = Some(ExecutorSelector {
        name: Some("bglab-ub".into()),
        ..ExecutorSelector::default()
    });
    let refusal = choose_executor(&HashMap::new(), &request).unwrap_err();
    assert!(
        refusal.contains("no executor is currently attached"),
        "{refusal}"
    );
}

/// The home pin is the runner's own placement fact, not a selector: it is
/// honored exactly, and it is not something a requester can state.
#[test]
fn a_pinned_batch_reaches_only_the_machine_holding_its_tree() {
    let connections = HashMap::from([
        fleet_entry("remote-a", "linux", 0, &[]),
        fleet_entry("remote-b", "linux", 0, &[]),
    ]);
    let mut request = targeted_request("linux");
    request.executor = None;
    request.pinned_executor_id = Some("remote-b".into());
    assert_eq!(
        choose_executor(&connections, &request)
            .unwrap()
            .unwrap()
            .executor_id,
        "remote-b"
    );

    // A pin to a machine that is gone refuses, and the refusal still lists
    // the fleet an agent could read.
    request.pinned_executor_id = Some("remote-c".into());
    let refusal = choose_executor(&connections, &request).unwrap_err();
    assert!(refusal.contains("working tree"), "{refusal}");
    assert!(refusal.contains("cairn://executors"), "{refusal}");
}

/// When a batch is both pinned to its job's home and carrying a selector of
/// its own, both are why nothing matched. Reporting only the pin tells an
/// agent its batch was misplaced when what actually failed is the request it
/// wrote — and leaves the word it typed out of the answer entirely.
#[test]
fn a_pinned_batch_that_also_asked_for_something_is_refused_in_its_own_words() {
    let connections = HashMap::from([fleet_entry("remote-a", "linux", 0, &[])]);
    let mut request = targeted_request("plan9");
    request.pinned_executor_id = Some("remote-a".into());

    let refusal = choose_executor(&connections, &request).unwrap_err();

    assert!(refusal.contains("working tree"), "{refusal}");
    assert!(refusal.contains("plan9"), "{refusal}");
    assert!(refusal.contains("remote-a"), "{refusal}");
    assert!(refusal.contains("cairn://executors"), "{refusal}");
}

/// Untargeted routing is unchanged by any of this: a batch that names no
/// machine still takes the colocated compatibility path without consulting a
/// selector, which is the seam CAIRN-3323 builds policy on.
#[test]
fn an_untargeted_batch_still_routes_to_the_colocated_executor() {
    let connections = HashMap::from([
        fleet_entry(COLOCATED_EXECUTOR_ID, "macos", 5, &[]),
        fleet_entry("remote-a", "linux", 0, &[]),
    ]);
    let mut request = targeted_request("linux");
    request.executor = None;
    assert_eq!(
        choose_executor(&connections, &request)
            .unwrap()
            .unwrap()
            .executor_id,
        COLOCATED_EXECUTOR_ID
    );
}

/// The projection an agent reads is deterministic, addressed by name, and
/// carries each executor's own occupancy rather than a fleet aggregate.
#[test]
fn the_inspection_projection_is_name_ordered_and_per_machine() {
    let pool = Fleet::default();
    let mut colocated = fleet_entry(COLOCATED_EXECUTOR_ID, "macos", 0, &[]).1;
    Arc::make_mut(&mut colocated.snapshot).executing_requests = vec![ExecutingCellRequest {
        command_resource_identity: None,
        executor_id: COLOCATED_EXECUTOR_ID.into(),
        cell_id: "cell".into(),
        request_id: "r".into(),
        attempt_id: "a".into(),
        owner: None,
        command_class: cairn_common::executor_protocol::CellCommandClass::Other,
        command: "true".into(),
        started_at_unix_ms: 1,
        process_ids: Vec::new(),
        priority: None,
        subscriber_count: 1,
        resource_reservation: ResourceReservation::default(),
        learned_estimate: None,
        live_usage: None,
        build_jobs: None,
        adopted: None,
        memory_phase: None,
        standalone: false,
    }];
    let mut remote = fleet_entry("remote-a", "linux", 0, &[]).1;
    remote.identity.display_name = "BGLab UB".into();
    pool.lock_connections().extend([
        (COLOCATED_EXECUTOR_ID.to_string(), colocated),
        ("remote-a".to_string(), remote),
    ]);

    let inspected = pool.inspect_executors(NOW + 5_000);

    // Sorted by public address, and the runner's own executor answers to the
    // reserved name whatever its label says.
    assert_eq!(
        inspected
            .iter()
            .map(|executor| executor.name.as_str())
            .collect::<Vec<_>>(),
        vec!["bglab-ub", "local"]
    );
    assert!(inspected[1].colocated);
    // Occupancy is attributed to the machine holding it, not summed.
    assert_eq!(inspected[1].occupancy.executing_requests.len(), 1);
    assert!(inspected[0].occupancy.executing_requests.is_empty());
    // Ages derive from the one capture instant handed in.
    assert_eq!(inspected[0].captured_at_unix_ms, NOW + 5_000);
    assert_eq!(inspected[0].health.heartbeat_age_ms, 5_000);
    assert_eq!(
        pool.executor_public_name(COLOCATED_EXECUTOR_ID).as_deref(),
        Some("local")
    );
    assert_eq!(pool.executor_public_name("absent"), None);
}

/// The telemetry frame is built straight from the connection table so it
/// need not clone every snapshot, and it must still be exactly the frame the
/// inspection projection renders: online or stale, busy or idle, measured
/// or not.
#[test]
fn the_telemetry_frame_matches_the_inspection_projection() {
    let pool = Fleet::default();
    let mut busy = fleet_entry(COLOCATED_EXECUTOR_ID, "macos", 0, &[]).1;
    Arc::make_mut(&mut busy.snapshot).executing_requests = vec![ExecutingCellRequest {
        command_resource_identity: None,
        executor_id: COLOCATED_EXECUTOR_ID.into(),
        cell_id: "cell".into(),
        request_id: "r".into(),
        attempt_id: "a".into(),
        owner: None,
        command_class: cairn_common::executor_protocol::CellCommandClass::Other,
        command: "true".into(),
        started_at_unix_ms: 1,
        process_ids: Vec::new(),
        priority: None,
        subscriber_count: 1,
        resource_reservation: ResourceReservation::default(),
        learned_estimate: None,
        live_usage: None,
        build_jobs: None,
        adopted: None,
        memory_phase: None,
        standalone: false,
    }];
    Arc::make_mut(&mut busy.health).machine.memory =
        cairn_common::wire_vocabulary::Measurement::measured(
            NOW,
            cairn_common::executor_protocol::MachineMemory {
                total_bytes: 1_000,
                available_bytes: 400,
            },
        );
    let mut quiet = fleet_entry("remote-a", "linux", 0, &[]).1;
    quiet.identity.display_name = "BGLab UB".into();
    pool.lock_connections().extend([
        (COLOCATED_EXECUTOR_ID.to_string(), busy),
        ("remote-a".to_string(), quiet),
    ]);

    for captured_at in [NOW + 5_000, NOW + EXECUTOR_LINK_STALE_AFTER_MS + 1] {
        let frame = pool.telemetry_frame(captured_at);
        assert_eq!(
            frame,
            telemetry::FleetTelemetryFrame::from_inspections(
                &pool.inspect_executors(captured_at),
                captured_at
            )
        );
        assert_eq!(frame.executors[1].running, 1);
        assert_eq!(frame.executors[1].mem_percent, Some(60.0));
    }
    assert!(pool.telemetry_frame(NOW + 5_000).executors[0].online);
    assert!(
        !pool
            .telemetry_frame(NOW + EXECUTOR_LINK_STALE_AFTER_MS + 1)
            .executors[0]
            .online
    );
}

/// A public name is an address, so configuration cannot introduce two
/// machines answering to one, and no remote may claim the reserved local
/// name.
#[test]
fn fleet_configuration_keeps_public_names_unique_and_reserves_local() {
    let remote = |id: &str, display: &str| {
        let mut config = darwin_remote_config();
        config.executor_id = id.into();
        config.device_id = format!("{id}-device");
        config.display_name = display.into();
        (id.to_string(), config)
    };

    let mut config = FleetConfig {
        remote_executors: BTreeMap::from([remote("one", "BGLab UB"), remote("two", "bglab-ub")]),
        ..FleetConfig::default()
    };
    let error = config.validate().unwrap_err();
    assert!(error.contains("bglab-ub"), "{error}");
    assert!(error.contains("cairn executor rename"), "{error}");

    config.remote_executors = BTreeMap::from([remote("one", "local")]);
    assert!(config
        .validate()
        .unwrap_err()
        .contains("reserved name local"));

    config.remote_executors = BTreeMap::from([remote("one", "---")]);
    assert!(config.validate().unwrap_err().contains("no public name"));

    config.remote_executors =
        BTreeMap::from([remote("one", "BGLab UB"), remote("two", "bglab-mac")]);
    assert!(config.validate().is_ok());
}

#[test]
fn snapshot_aggregates_lifetime_count_and_reservations_across_executors() {
    let pool = Fleet::default();
    let mut first = fleet_entry("first", "macos", 0, &[]).1;
    Arc::make_mut(&mut first.snapshot).resident_occupancy = Some(ResidentOccupancyEvidence {
        process_count: 2,
        reservation: ResourceReservation {
            memory_bytes: 1_000,
            disk_growth_bytes: 2_000,
            concurrency_units: 3,
            source: ResourceReservationSource::Declared,
            measured_peak_bytes: None,
            test_phase: None,
        },
    });
    let mut second = fleet_entry("second", "linux", 0, &[]).1;
    Arc::make_mut(&mut second.snapshot).resident_occupancy = Some(ResidentOccupancyEvidence {
        process_count: 1,
        reservation: ResourceReservation {
            memory_bytes: 4_000,
            disk_growth_bytes: 8_000,
            concurrency_units: 5,
            source: ResourceReservationSource::Declared,
            measured_peak_bytes: None,
            test_phase: None,
        },
    });
    pool.lock_connections()
        .extend([("first".into(), first), ("second".into(), second)]);

    let occupancy = pool.snapshot().resident_occupancy.unwrap();
    assert_eq!(occupancy.process_count, 3);
    assert_eq!(occupancy.reservation.memory_bytes, 5_000);
    assert_eq!(occupancy.reservation.disk_growth_bytes, 10_000);
    assert_eq!(occupancy.reservation.concurrency_units, 8);
}

#[test]
fn cached_completion_history_is_explicit_bounded_and_newest_first() {
    let pool = Fleet::default();
    for index in 0..40 {
        pool.record_cached_completion(
            "project",
            "job",
            Some("executor"),
            &format!("check-{index}"),
            CellPriority::ReviewCheck,
            true,
        );
        std::thread::sleep(Duration::from_millis(1));
    }

    let recent = pool.snapshot().recent_completions;
    assert_eq!(recent.len(), 32);
    assert_eq!(recent[0].command, "check-39");
    assert!(recent[0].cached);
    assert_eq!(recent[0].duration_ms, 0);
    assert!(recent[0].resource_reservation.is_none());
}

#[test]
fn executor_health_keeps_stale_executor_with_capture_time_age() {
    let pool = Fleet::default();
    let (executor_id, connection) = fleet_entry("stale", "macos", 0, &[]);
    pool.lock_connections().insert(executor_id, connection);

    let health = pool.executor_health(NOW + 120_000);
    assert_eq!(health.len(), 1);
    assert_eq!(health[0].identity.executor_id, "stale");
    assert_eq!(health[0].status, ExecutorHealthStatus::Stale);
    assert_eq!(health[0].heartbeat_age_ms, 120_000);
}

/// A machine whose readings are fresh but whose link has gone silent is a
/// connection problem, and the snapshot keeps the last measurements it was
/// given rather than discarding them.
#[test]
fn a_stale_connection_keeps_the_measurements_it_last_carried() {
    let pool = Fleet::default();
    let (executor_id, connection) = fleet_entry("gone-quiet", "macos", 0, &[]);
    let mut advertisement = connection.advertisement.as_ref().clone();
    pool.lock_connections()
        .insert(executor_id.clone(), connection);

    let beat_at = 1_000_000;
    advertisement.observed_at_unix_ms = beat_at;
    advertisement.liveness_observed_at_unix_ms = Some(beat_at);
    let mut health = ExecutorSubstrateReport::default();
    health.machine.memory = Measurement::measured(
        beat_at,
        MachineMemory {
            total_bytes: 32_000,
            available_bytes: 12_000,
        },
    );
    assert!(pool.handle_executor_message(
        &executor_id,
        1,
        ExecutorMessage::Heartbeat {
            advertisement,
            health,
            last_update: None,
            machine: None,
        },
    ));

    let silent = pool.executor_health(beat_at + 120_000).remove(0);
    assert_eq!(silent.status, ExecutorHealthStatus::Stale);
    assert!(
        silent.telemetry_stale,
        "a link that stopped delivering also stopped delivering fresh facts"
    );
    assert_eq!(
        silent.machine.memory.value().unwrap().available_bytes,
        12_000,
        "the last measurement is still the last measurement"
    );
    assert_eq!(
        silent.machine.memory.age_ms(beat_at + 120_000),
        120_000,
        "its age is computed from when it was taken, not from the beat"
    );
}

#[test]
fn executor_health_separates_the_runner_own_executor_from_enrolled_ones() {
    let pool = Fleet::default();
    let (colocated_sender, _colocated_receiver) = mpsc::unbounded_channel();
    pool.attach_executor(colocated_sender);
    let (_, managed) = fleet_entry("managed", "linux", 0, &[]);
    pool.attach_advertised_executor(
        Arc::unwrap_or_clone(managed.advertisement),
        Default::default(),
        managed.sender,
        false,
        None,
        None,
    )
    .expect("attach");

    let attribution: Vec<_> = pool
        .executor_health(1)
        .into_iter()
        .map(|executor| (executor.identity.executor_id, executor.colocated))
        .collect();
    assert_eq!(
        attribution,
        vec![
            (COLOCATED_EXECUTOR_ID.to_string(), true),
            ("managed".to_string(), false),
        ]
    );
}

#[test]
fn build_skew_compares_the_running_executor_to_the_runner_deployed_artifact() {
    let pool = Fleet::default();
    let (executor_id, mut connection) = fleet_entry("colocated", "macos", 0, &[]);
    connection.executor_build_id = Some("running-build".into());
    pool.lock_connections()
        .insert(executor_id.clone(), connection);

    assert!(pool.executor_health(1)[0].build_skew.is_none());
    pool.set_expected_executor_build_id(executor_id, "deployed-build".into());
    let skew = pool.executor_health(1)[0].build_skew.clone().unwrap();
    assert_eq!(skew.runner_build_id, "deployed-build");
    assert_eq!(skew.executor_build_id, "running-build");
}

/// cairn/6145: what the machines report about a request placed before a
/// restart. Attachment alone says nothing; once every machine has delivered
/// a snapshot on its link, an attempt it lists is held and one absent from
/// every report is gone.
#[test]
fn a_request_report_speaks_only_once_every_machine_has_reported() {
    let pool = Fleet::default();
    let (executor_id, mut connection) = fleet_entry("bbs", "linux", 0, &[]);
    connection.snapshots_received = 0;
    Arc::make_mut(&mut connection.snapshot).granted_attempts =
        vec![cairn_common::executor_protocol::GrantedAttempt {
            request_id: "held".into(),
            attempt_id: "a".into(),
        }];
    pool.lock_connections()
        .insert(executor_id.clone(), connection);
    assert!(
        matches!(
            pool.request_report("gone", None),
            RequestReport::Unknown { .. }
        ),
        "an attached machine that has not reported proves nothing"
    );

    pool.lock_connections()
        .get_mut(&executor_id)
        .unwrap()
        .snapshots_received = 1;
    assert_eq!(
        pool.request_report("held", None),
        RequestReport::Held {
            executor_id: executor_id.clone()
        }
    );
    assert_eq!(pool.request_report("gone", None), RequestReport::Gone);
    assert!(
        !pool.is_request_in_flight("held"),
        "the registry has no subscriber for it, which is why the report is asked"
    );
}

/// cairn/6991: a request whose holder is known is judged by that machine's
/// report alone. Another enrolled machine being away (bgsrv-ub2, offline
/// for days) no longer leaves every restart-detached batch unknown; the
/// holder away names itself as what the request waits for; and with no
/// known holder the away machine is named instead.
#[test]
fn a_request_report_judges_the_machine_that_claimed_it() {
    let pool = Fleet::default();
    let (holder, mut connection) = fleet_entry("bgsrv-mac", "macos", 0, &[]);
    connection.snapshots_received = 1;
    let holder_name = executor_public_name(&connection);
    pool.lock_connections().insert(holder.clone(), connection);
    pool.declare_enrolled_remote(&holder, &holder_name, "macos", "aarch64");
    pool.declare_enrolled_remote("bgsrv-ub2", "bgsrv-ub2", "linux", "x86_64");

    assert_eq!(
        pool.request_report("gone", Some(&holder)),
        RequestReport::Gone,
        "its holder reported without it; another machine being away is irrelevant"
    );
    assert_eq!(
        pool.request_report("gone", None),
        RequestReport::Unknown {
            awaiting: vec!["bgsrv-ub2".to_string()]
        },
        "with no known holder, the away machine could hold it"
    );
    assert_eq!(
        pool.request_report("gone", Some("bgsrv-ub2")),
        RequestReport::Unknown {
            awaiting: vec!["bgsrv-ub2".to_string()]
        }
    );
    assert_eq!(
        pool.request_report("gone", Some("retired-machine")),
        RequestReport::Gone,
        "a holder no longer enrolled can never report again"
    );

    pool.lock_connections()
        .get_mut(&holder)
        .unwrap()
        .snapshots_received = 0;
    assert_eq!(
        pool.request_report("gone", Some(&holder)),
        RequestReport::Unknown {
            awaiting: vec![holder_name]
        },
        "an attached holder that has not reported proves nothing"
    );
}

/// A remote running a different build from the one the runner serves for
/// its target is refused new work, and directed over its own link only once
/// its work has drained and two snapshots since the hold say so.
#[test]
fn a_build_skewed_remote_is_held_then_directed_over_its_link_once_empty() {
    use cairn_common::executor_protocol::SelfUpdatePhase;

    let pool = Fleet::default();
    let (executor_id, mut connection) = fleet_entry("bbs", "linux", 0, &[]);
    let (sender, mut executor) = mpsc::unbounded_channel();
    connection.sender = sender;
    connection.executor_build_id = Some("build-0759".into());
    connection.link_clock.stage(0, 0);
    Arc::make_mut(&mut connection.snapshot).granted_attempts =
        vec![cairn_common::executor_protocol::GrantedAttempt {
            request_id: "r".into(),
            attempt_id: "a".into(),
        }];
    pool.lock_connections()
        .insert(executor_id.clone(), connection);
    pool.record_executor_artifact_build("x86_64-unknown-linux-gnu", Ok("build-1329".into()));

    assert!(pool
        .advance_self_update_rollout(NOW, |_| Err("injected precommit failure".into()))
        .is_err());
    assert!(!pool.self_update.lock().unwrap().holds(&executor_id));
    assert!(
        !pool
            .lock_connections()
            .get(&executor_id)
            .unwrap()
            .update_hold
    );
    assert!(
        executor.try_recv().is_err(),
        "a failed drain commit sends no directive"
    );
    pool.advance_self_update_rollout(NOW, |_| Ok(())).unwrap();
    assert!(
        pool.lock_connections()
            .get(&executor_id)
            .unwrap()
            .update_hold
    );
    let inspected = pool.inspect_executors(NOW);
    assert!(matches!(
        &inspected[0].self_update.as_ref().unwrap().phase,
        SelfUpdatePhase::Draining {
            in_flight: 1,
            since_unix_ms: NOW,
            holds,
            ..
        } if holds[0].label == "request r (preparing its cell)"
    ));
    assert_eq!(
        inspected[0]
            .health
            .build_skew
            .as_ref()
            .unwrap()
            .runner_build_id,
        "build-1329",
        "a remote's expected build is what the runner serves for its target"
    );
    assert!(
        executor.try_recv().is_err(),
        "nothing is directed while work is in flight"
    );

    {
        let mut connections = pool.lock_connections();
        let entry = connections.get_mut(&executor_id).unwrap();
        Arc::make_mut(&mut entry.snapshot).granted_attempts.clear();
        entry.snapshots_received += 2;
        entry.link_clock.stage(0, 0);
    }
    assert!(pool
        .advance_self_update_rollout(NOW + 30_000, |_| Err("injected precommit failure".into()))
        .is_err());
    assert!(pool.self_update.lock().unwrap().holds(&executor_id));
    assert!(
        pool.lock_connections()
            .get(&executor_id)
            .unwrap()
            .update_hold
    );
    assert!(
        executor.try_recv().is_err(),
        "a failed directive transition preserves the hold and sends nothing"
    );
    pool.advance_self_update_rollout(NOW + 30_000, |_| Ok(()))
        .unwrap();
    let directed =
        std::iter::from_fn(|| executor.try_recv().ok()).find_map(|message| match message {
            ExecutorMessage::UpdateDirected {
                manifest_url,
                build_id,
            } => Some((manifest_url, build_id)),
            _ => None,
        });
    assert_eq!(
        directed,
        Some((
            cairn_common::executor_protocol::EXECUTOR_ARTIFACT_MANIFEST_PATH.to_string(),
            "build-1329".to_string()
        ))
    );
    assert!(
        pool.lock_connections()
            .get(&executor_id)
            .unwrap()
            .update_hold,
        "held until it returns on the new build"
    );
}

/// Being emptied for an update ends on its own, so work only that machine
/// could take waits for it rather than dying for want of a machine.
#[test]
fn placement_waits_on_a_machine_emptied_for_its_update() {
    let mut connections = HashMap::from([
        fleet_entry(COLOCATED_EXECUTOR_ID, "macos", 0, &[]),
        fleet_entry("bbs", "linux", 0, &[]),
    ]);
    connections
        .get_mut(COLOCATED_EXECUTOR_ID)
        .unwrap()
        .colocated = true;
    runtime_update_hold_fixture(connections.get_mut("bbs").unwrap(), true);
    let mut request = targeted_request("linux");
    request.executor = Some(ExecutorSelector {
        name: Some("bbs".into()),
        ..ExecutorSelector::default()
    });
    let draft = place(&connections, &request)
        .expect("a machine being emptied for its update is waited on, not refused");
    assert!(draft.selected.is_none(), "it takes no new work meanwhile");
    assert!(
        draft
            .rejected
            .iter()
            .any(|rejection| rejection.executor_name == "bbs"
                && matches!(
                    rejection.reason,
                    PlacementRejectionReason::UpdatePending { .. }
                )),
        "{:?}",
        draft.rejected
    );
    // What the requester reads if its horizon arrives first.
    assert!(
        describe_declines(&draft.rejected)
            .contains("Waited on: bbs being emptied for an executor update"),
        "{}",
        describe_declines(&draft.rejected)
    );

    runtime_update_hold_fixture(connections.get_mut("bbs").unwrap(), false);
    assert_eq!(
        place(&connections, &request)
            .unwrap()
            .selected
            .expect("the hold's end re-admits it")
            .0
            .executor_id,
        "bbs"
    );
}

/// cairn/6382's acceptance: a job resident on a machine draining for its
/// update runs its next batch there, on the outgoing build, and the update
/// waits for it like any other in-flight command. Work that would make a
/// new environment there is still held.
#[test]
fn an_update_hold_admits_work_pinned_to_the_execution_home_it_already_hosts() {
    let (executor_id, mut resident_home) = fleet_entry("bbs", "linux", 0, &[]);
    runtime_update_hold_fixture(&mut resident_home, true);
    let connections = HashMap::from([(executor_id, resident_home)]);
    let mut resident = targeted_request("linux");
    resident.pinned_executor_id = Some("bbs".into());
    let selected = place(&connections, &resident)
        .expect("resident work crosses the update hold")
        .selected
        .expect("its home takes it")
        .0;
    assert_eq!(selected.executor_id, "bbs");

    let mut named = targeted_request("linux");
    named.executor = Some(ExecutorSelector {
        name: Some("bbs".into()),
        ..ExecutorSelector::default()
    });
    let draft = place(&connections, &named).expect("new work waits for the update");
    assert!(draft.selected.is_none(), "new work is still held");
}

/// The resident exception belongs to the self-update effect alone. A check
/// lane pinned to the home builds a new cell, so it waits for the update;
/// an operator drain beside the update refuses the resident batch too, and
/// only then does the executor's own boolean refuse.
#[test]
fn only_the_update_drain_admits_resident_work_and_only_alone() {
    let (executor_id, mut resident_home) = fleet_entry("bbs", "linux", 0, &[]);
    runtime_update_hold_fixture(&mut resident_home, true);
    assert!(!drain_projection::protocol_refusal(
        &resident_home.drain_blockers
    ));
    let mut resident = targeted_request("linux");
    resident.pinned_executor_id = Some("bbs".into());
    let mut check = resident.clone();
    check.placement_work_class = PlacementWorkClass::WriteChecks;
    assert_eq!(
        candidate_rejection(&resident_home, PlacementScope::of(&check), NOW),
        Some(PlacementRejectionReason::UpdatePending {
            returns_by_unix_ms: None
        }),
        "a pinned check lane is a new environment"
    );

    published_drains_fixture(&mut resident_home, true, true);
    assert!(drain_projection::protocol_refusal(
        &resident_home.drain_blockers
    ));
    assert_eq!(
        candidate_rejection(&resident_home, PlacementScope::of(&resident), NOW),
        Some(PlacementRejectionReason::Drained),
        "an operator drain refuses pinned work whatever stands beside it"
    );
    let connections = HashMap::from([(executor_id, resident_home)]);
    assert!(place(&connections, &resident)
        .map(|draft| draft.selected.is_none())
        .unwrap_or(true));

    let (_, mut operator_only) = fleet_entry("bbs", "linux", 0, &[]);
    published_drains_fixture(&mut operator_only, false, true);
    assert!(drain_projection::protocol_refusal(
        &operator_only.drain_blockers
    ));
    assert_eq!(
        candidate_rejection(&operator_only, PlacementScope::of(&resident), NOW),
        Some(PlacementRejectionReason::Drained)
    );
}

/// cairn/6382's misattribution: a batch pinned to its home on bgsrv-ub,
/// waiting there for room, was told it waited on bbs being emptied for an
/// update. A machine the pin excludes says so, never its hold.
#[test]
fn a_pin_elsewhere_is_not_reported_as_waiting_on_another_machines_update() {
    let mut connections = HashMap::from([
        fleet_entry("bbs", "linux", 0, &[]),
        fleet_entry("bgsrv-ub", "linux", 0, &[]),
    ]);
    runtime_update_hold_fixture(connections.get_mut("bbs").unwrap(), true);
    connections.get_mut("bgsrv-ub").unwrap().drained = true;
    let mut request = targeted_request("linux");
    request.pinned_executor_id = Some("bgsrv-ub".into());
    let rejected = match place(&connections, &request) {
        Ok(draft) => draft.rejected,
        Err(_) => survey_candidates(&connections, &[], &request, NOW)
            .err()
            .map(|_| Vec::new())
            .unwrap_or_default(),
    };
    assert!(
        !rejected
            .iter()
            .any(|rejection| rejection.executor_id == "bbs"
                && matches!(
                    rejection.reason,
                    PlacementRejectionReason::UpdatePending { .. }
                )),
        "{rejected:?}"
    );
    let survey = HashMap::from([connections
        .remove("bbs")
        .map(|entry| ("bbs".to_string(), entry))
        .unwrap()]);
    let reason = candidate_rejection(&survey["bbs"], PlacementScope::of(&request), NOW);
    assert!(
        matches!(reason, Some(PlacementRejectionReason::PinMismatch { .. })),
        "{reason:?}"
    );
}

/// A machine that closes its link to install the runner's build is about
/// to return. Work that named it waits for that rather than being refused
/// the moment the update hold is about to end.
#[tokio::test]
async fn work_naming_a_machine_installing_its_update_waits_for_its_return() {
    let pool = Fleet::default();
    pool.declare_enrolled_remote("bbs", "bbs", "linux", "x86_64");
    let now = unix_time_ms();
    let (executor_id, mut connection) = fleet_entry_at("bbs", "linux", 0, &[], now);
    let (sender, mut executor) = mpsc::unbounded_channel();
    connection.sender = sender;
    connection.executor_build_id = Some("build-0759".into());
    connection.snapshots_received = 0;
    pool.lock_connections()
        .insert(executor_id.clone(), connection);
    pool.record_executor_artifact_build("x86_64-unknown-linux-gnu", Ok("build-1329".into()));
    pool.advance_self_update_rollout(now, |_| Ok(())).unwrap();
    pool.lock_connections()
        .get_mut(&executor_id)
        .unwrap()
        .snapshots_received += 2;
    pool.advance_self_update_rollout(now + 30_000, |_| Ok(()))
        .unwrap();
    assert!(
        std::iter::from_fn(|| executor.try_recv().ok())
            .any(|message| matches!(message, ExecutorMessage::UpdateDirected { .. })),
        "the empty machine is directed"
    );
    pool.lock_connections().remove(&executor_id);

    let mut request = targeted_request("linux");
    request.repository = spillable_request().repository;
    request.executor = Some(ExecutorSelector {
        name: Some("bbs".into()),
        ..ExecutorSelector::default()
    });
    request.wait_horizon_unix_ms = unix_time_ms() + 400;
    let refusal = place_through_dispatcher(&pool, &request, None)
        .await
        .expect_err("bbs does not return inside the horizon");
    let CellOutcome::Unavailable { diagnostic, .. } = refusal else {
        panic!("expected a refusal at the horizon, got {refusal:?}");
    };
    assert!(
        diagnostic.contains("wait horizon"),
        "waited rather than refused at once: {diagnostic}"
    );
}

/// A capable Mac and one whose check lanes run under the operator
/// credential floor, as both evidence shapes are published.
fn capable_and_floored_evidence() -> (SandboxCapabilityEvidence, SandboxCapabilityEvidence) {
    let context = cairn_common::executor_protocol::NestedSandboxProbeContext {
        launcher: cairn_common::executor_protocol::SandboxLauncherClass::Unconfined,
        credential_floor_sha256: None,
        outer_profile_sha256: None,
        fingerprint: Some("f".into()),
        measured_at_unix_ms: 1,
    };
    let capable = SandboxCapabilityEvidence {
        cairn_sandboxed_marker: false,
        nested_sandbox_write_available: true,
        nested_sandbox_write_detail: "live probe succeeded".into(),
        nested_sandbox_write_context: Some(context.clone()),
        violation_recovery: None,
    };
    let floored = SandboxCapabilityEvidence {
        nested_sandbox_write_available: false,
        nested_sandbox_write_detail:
            "outer profile inherited: a confined process cannot apply a second profile".into(),
        nested_sandbox_write_context: Some(
            cairn_common::executor_protocol::NestedSandboxProbeContext {
                credential_floor_sha256: Some("f1007f1007f1007".into()),
                outer_profile_sha256: Some("outer".into()),
                ..context
            },
        ),
        ..capable.clone()
    };
    (capable, floored)
}

fn set_sandbox(
    connections: &mut HashMap<String, ExecutorConnectionState>,
    executor_id: &str,
    evidence: Option<SandboxCapabilityEvidence>,
) {
    Arc::make_mut(&mut connections.get_mut(executor_id).unwrap().advertisement)
        .capabilities
        .sandbox = evidence;
}

fn nested_sandbox_lane(os: &str) -> CellRequest {
    let mut request = targeted_request(os);
    request.repository = RepositoryLocator::ManagedObjects {
        project_id: "p".into(),
        repository_id: "repo".into(),
        object_format: GitObjectFormat::Sha1,
    };
    request.placement_mobility = PlacementMobility::SpillEligible;
    request.executor = Some(ExecutorSelector {
        os: Some(os.into()),
        required_toolchains: vec!["rust".into()],
        requires_macos_nested_sandbox_write: true,
        ..ExecutorSelector::default()
    });
    request
}

/// The waiver is for the floor alone. A Mac whose executor is itself
/// confined, or that could not tell, is refused as before, and so is one
/// that measured from its own process.
#[test]
fn only_the_credential_floor_is_waived() {
    let (_, floored) = capable_and_floored_evidence();
    let selector = ExecutorSelector {
        requires_macos_nested_sandbox_write: true,
        ..ExecutorSelector::default()
    };
    let mut capabilities = capabilities_with_cores(8);
    capabilities.sandbox = Some(floored.clone());
    assert_eq!(
        nested_sandbox_fit(&capabilities, &selector),
        NestedSandboxFit::Waivable
    );
    assert!(!matches_sandbox_capability(&capabilities, &selector));

    for launcher in [
        cairn_common::executor_protocol::SandboxLauncherClass::ConfinedDescendant,
        cairn_common::executor_protocol::SandboxLauncherClass::Unknown,
    ] {
        let mut evidence = floored.clone();
        evidence
            .nested_sandbox_write_context
            .as_mut()
            .unwrap()
            .launcher = launcher;
        capabilities.sandbox = Some(evidence);
        assert_eq!(
            nested_sandbox_fit(&capabilities, &selector),
            NestedSandboxFit::Refused,
            "{launcher:?}"
        );
    }
    let mut unfloored = floored.clone();
    unfloored
        .nested_sandbox_write_context
        .as_mut()
        .unwrap()
        .credential_floor_sha256 = None;
    capabilities.sandbox = Some(unfloored);
    assert_eq!(
        nested_sandbox_fit(&capabilities, &selector),
        NestedSandboxFit::Refused
    );
    let mut contextless = floored;
    contextless.nested_sandbox_write_context = None;
    capabilities.sandbox = Some(contextless);
    assert_eq!(
        nested_sandbox_fit(&capabilities, &selector),
        NestedSandboxFit::Refused
    );
}

/// Linux placement for the same lane is untouched: a Linux machine
/// satisfies the requirement, records no waiver, and holds a floored Mac
/// back from a lane that would count either platform.
#[test]
fn the_nested_sandbox_preference_leaves_linux_placement_alone() {
    let (_, floored) = capable_and_floored_evidence();
    let mut connections = HashMap::from([
        fleet_entry("mac-floor", "macos", 0, &[]),
        fleet_entry("fedora", "linux", 4, &[]),
    ]);
    set_sandbox(&mut connections, "mac-floor", Some(floored));

    let draft = place(&connections, &nested_sandbox_lane("linux")).expect("linux places");
    let (selected, selection) = draft.selected.expect("placed");
    assert_eq!(selected.executor_id, "fedora");
    assert_eq!(selection.nested_sandbox_waived, None);

    let mut either = nested_sandbox_lane("linux");
    either.executor.as_mut().unwrap().os = None;
    let draft = place(&connections, &either).expect("either platform places");
    assert_eq!(draft.selected.expect("placed").0.executor_id, "fedora");
}

#[test]
fn heartbeat_refreshes_live_executor_health_report() {
    let pool = Fleet::default();
    let (executor_id, connection) = fleet_entry("live", "macos", 0, &[]);
    let mut advertisement = connection.advertisement.as_ref().clone();
    pool.lock_connections()
        .insert(executor_id.clone(), connection);

    let mut first = ExecutorSubstrateReport::default();
    first.machine.memory = Measurement::measured(
        1,
        MachineMemory {
            total_bytes: 8_000,
            available_bytes: 4_000,
        },
    );
    assert!(pool.handle_executor_message(
        &executor_id,
        1,
        ExecutorMessage::Heartbeat {
            advertisement: advertisement.clone(),
            health: first,
            last_update: None,
            machine: None,
        },
    ));
    assert_eq!(
        pool.executor_health(1)[0]
            .machine
            .memory
            .value()
            .unwrap()
            .available_bytes,
        4_000
    );

    advertisement.observed_at_unix_ms = 2;
    let mut second = ExecutorSubstrateReport::default();
    second.machine.memory = Measurement::measured(
        2,
        MachineMemory {
            total_bytes: 8_000,
            available_bytes: 2_000,
        },
    );
    second.disk.status = cairn_common::executor_protocol::DiskHealthStatus::Full;
    assert!(pool.handle_executor_message(
        &executor_id,
        1,
        ExecutorMessage::Heartbeat {
            advertisement,
            health: second,
            last_update: None,
            machine: None,
        },
    ));
    let health = pool.executor_health(2);
    assert_eq!(
        health[0].machine.memory.value().unwrap().available_bytes,
        2_000
    );
    assert_eq!(
        health[0].disk.status,
        cairn_common::executor_protocol::DiskHealthStatus::Full
    );
}

/// The price of emitting beats from a task that computes nothing is that a
/// wedged producer keeps beating. A beat arriving on schedule while the
/// facts it carries were measured ten minutes ago is not evidence of a
/// healthy executor, and the snapshot has to say so on the payload's age
/// alone. It must also keep reporting the heartbeat itself as fresh,
/// because the link genuinely is: conflating the two would make every
/// healthy executor look stale to the connection-health surfaces.
#[test]
fn a_beat_on_schedule_carrying_facts_that_stopped_moving_is_not_online() {
    let pool = Fleet::default();
    let (executor_id, connection) = fleet_entry("wedged", "macos", 0, &[]);
    let mut advertisement = connection.advertisement.as_ref().clone();
    pool.lock_connections()
        .insert(executor_id.clone(), connection);

    let beat_at = 1_000_000;
    advertisement.observed_at_unix_ms = beat_at;
    advertisement.liveness_observed_at_unix_ms = Some(beat_at);
    assert!(pool.handle_executor_message(
        &executor_id,
        1,
        ExecutorMessage::Heartbeat {
            advertisement: advertisement.clone(),
            health: ExecutorSubstrateReport::default(),
            last_update: None,
            machine: None,
        },
    ));
    let fresh = pool.executor_health(beat_at).remove(0);
    assert_eq!(fresh.status, ExecutorHealthStatus::Online);
    assert_eq!(fresh.liveness_age_ms, Some(0));

    // The publisher keeps its cadence. The refresher does not.
    let much_later = beat_at + 10 * 60 * 1_000;
    advertisement.observed_at_unix_ms = much_later;
    assert!(pool.handle_executor_message(
        &executor_id,
        1,
        ExecutorMessage::Heartbeat {
            advertisement,
            health: ExecutorSubstrateReport::default(),
            last_update: None,
            machine: None,
        },
    ));
    let wedged = pool.executor_health(much_later).remove(0);
    assert_eq!(
        wedged.heartbeat_age_ms, 0,
        "the link is alive and has to keep reading as alive"
    );
    assert_eq!(wedged.liveness_age_ms, Some(10 * 60 * 1_000));
    assert_eq!(
        wedged.status,
        ExecutorHealthStatus::Online,
        "the connection is not what failed here, and saying it did sends an \
             operator after the wrong problem"
    );
    assert!(
        wedged.telemetry_stale,
        "facts that stopped moving have to be reported as stale facts"
    );
}

/// An executor that makes no claim about when its facts were measured is
/// judged on its heartbeat alone. Absence of the claim is not evidence of
/// infinite staleness, or every executor predating the field would read as
/// permanently wedged.
#[test]
fn an_executor_that_reports_no_payload_age_is_judged_on_its_heartbeat() {
    let pool = Fleet::default();
    let (executor_id, connection) = fleet_entry("silent-about-age", "macos", 0, &[]);
    let mut advertisement = connection.advertisement.as_ref().clone();
    pool.lock_connections()
        .insert(executor_id.clone(), connection);

    let beat_at = 1_000_000;
    advertisement.observed_at_unix_ms = beat_at;
    advertisement.liveness_observed_at_unix_ms = None;
    assert!(pool.handle_executor_message(
        &executor_id,
        1,
        ExecutorMessage::Heartbeat {
            advertisement,
            health: ExecutorSubstrateReport::default(),
            last_update: None,
            machine: None,
        },
    ));
    let health = pool.executor_health(beat_at).remove(0);
    assert_eq!(health.liveness_age_ms, None);
    assert_eq!(health.status, ExecutorHealthStatus::Online);
    assert!(!health.telemetry_stale);
}

#[test]
fn repeated_executor_snapshot_refreshes_health_without_reporting_slot_change() {
    let pool = Fleet::default();
    let (executor_id, connection) = fleet_entry("snapshot", "macos", 0, &[]);
    pool.lock_connections()
        .insert(executor_id.clone(), connection);

    let snapshot = FleetSnapshot {
        resident_occupancy: Some(ResidentOccupancyEvidence {
            process_count: 1,
            reservation: ResourceReservation::default(),
        }),
        ..FleetSnapshot::default()
    };
    let mut first_health = ExecutorSubstrateReport::default();
    first_health.machine.memory = Measurement::measured(
        1,
        MachineMemory {
            total_bytes: 8_000,
            available_bytes: 4_000,
        },
    );
    assert!(pool.set_executor_snapshot(&executor_id, 1, snapshot.clone(), first_health));

    let mut second_health = ExecutorSubstrateReport::default();
    second_health.machine.memory = Measurement::measured(
        2,
        MachineMemory {
            total_bytes: 8_000,
            available_bytes: 2_000,
        },
    );
    assert!(!pool.set_executor_snapshot(&executor_id, 1, snapshot, second_health));
    assert_eq!(
        pool.executor_health(1)[0]
            .machine
            .memory
            .value()
            .unwrap()
            .available_bytes,
        2_000
    );
}

#[test]
fn empty_first_snapshot_reconciles_stale_persisted_route_without_public_change() {
    let temp = tempfile::tempdir().unwrap();
    let route_path = temp.path().join("lifetime-routes.json");
    let pool = Fleet::with_residency_route_path(route_path.clone());
    let executor_id = "snapshot".to_string();
    let request = dev_instance_request("stale-lease");
    pool.update_residency_routes(|known| {
        known.insert(
            (executor_id.clone(), request.holder.storage_key()),
            ResidencyRoute {
                holder: request.holder.clone(),
                repository: request.repository.clone(),
                executor_id: executor_id.clone(),
                pending: false,
            },
        );
    })
    .unwrap();

    let (_, connection) = fleet_entry(&executor_id, "macos", 0, &[]);
    let generation = pool
        .attach_advertised_executor(
            Arc::unwrap_or_clone(connection.advertisement),
            Default::default(),
            connection.sender,
            false,
            None,
            None,
        )
        .expect("attach");
    assert!(!pool.set_executor_snapshot(
        &executor_id,
        generation,
        FleetSnapshot::default(),
        ExecutorSubstrateReport::default(),
    ));
    assert!(pool.residency_routes.lock().unwrap().is_empty());

    drop(pool);
    assert!(Fleet::with_residency_route_path(route_path)
        .residency_routes
        .lock()
        .unwrap()
        .is_empty());
}

fn executing(command: &str, upper_duration_ms: u64) -> ExecutingCellRequest {
    ExecutingCellRequest {
        executor_id: String::new(),
        cell_id: "cell".into(),
        request_id: command.into(),
        attempt_id: "attempt".into(),
        owner: None,
        command_class: cairn_common::executor_protocol::CellCommandClass::CargoTest,
        command: command.into(),
        started_at_unix_ms: NOW,
        process_ids: Vec::new(),
        priority: None,
        subscriber_count: 1,
        resource_reservation: ResourceReservation::default(),
        command_resource_identity: None,
        learned_estimate: Some(cairn_common::executor_protocol::LearnedResourceEstimate {
            sample_count: 20,
            upper_duration_ms: Some(upper_duration_ms),
            upper_peak_rss_bytes: None,
            upper_disk_growth_bytes: None,
        }),
        live_usage: None,
        build_jobs: None,
        adopted: None,
        memory_phase: None,
        standalone: false,
    }
}

const FORECAST_PROJECT: &str = "p";

/// A colocated macOS machine that frees in four seconds, beside a remote
/// Linux one that does not free for five minutes.
fn occupied_fleet() -> Fleet {
    let pool = Fleet::default();
    for (id, os, colocated, occupant, relief_ms) in [
        ("local", "macos", true, executing("rust-fmt", 4_000), 4_000),
        (
            "bglab-ub",
            "linux",
            false,
            executing("rust-tests", 300_000),
            300_000,
        ),
    ] {
        let (executor_id, mut connection) = fleet_entry(id, os, 0, &[]);
        connection.colocated = colocated;
        Arc::make_mut(&mut connection.snapshot).executing_requests = vec![occupant];
        // The relief number is the machine's own, posted with its health.
        // The runner no longer derives it from the occupant list beside it;
        // the list is still here because it is what names the wait.
        Arc::make_mut(&mut connection.health).predicted_relief_ms = Some(relief_ms);
        pool.lock_connections().insert(executor_id, connection);
    }
    pool
}

/// A colocated check batch's scope: mobile, unpinned, over a colocated
/// checkout that can be recreated from managed objects elsewhere.
fn forecast_scope<'a>(
    repository: &'a RepositoryLocator,
    selector: Option<&'a ExecutorSelector>,
    mobility: PlacementMobility,
) -> PlacementScope<'a> {
    PlacementScope {
        project_id: FORECAST_PROJECT,
        repository,
        selector,
        pinned_executor_id: None,
        mobility,
        verdict_platforms: &[],
        avoided_executor_ids: &[],
        project_requirements: &NO_PROJECT_REQUIREMENTS,
        work_class: PlacementWorkClass::ReviewChecks,
        compiles_rust: false,
        pipe_floor: PipeFloorWork::Process,
        publication: None,
    }
}

static NO_PROJECT_REQUIREMENTS: ProjectPlacementRequirements = ProjectPlacementRequirements {
    setup_toolchains: Vec::new(),
    prerequisite_scripts: Vec::new(),
    setup_identity: String::new(),
    capabilities: Vec::new(),
};

fn forecast_repository() -> RepositoryLocator {
    RepositoryLocator::ColocatedPath {
        project_id: FORECAST_PROJECT.into(),
        repository_id: FORECAST_PROJECT.into(),
        absolute_path: "/repo".into(),
    }
}

/// Forecast fixtures share one synthetic instant for connection freshness,
/// occupant start time, and occupancy evaluation. Exact relief therefore
/// identifies which machine spoke without elapsed wall time making the
/// assertion scheduler-dependent.
#[track_caller]
fn assert_relief_near(occupancy: &occupancy::MachineOccupancy, expected_ms: u64, because: &str) {
    let occupancy::MachineOccupancy::Predicted(forecast) = occupancy else {
        panic!("{because}: expected a prediction, got {occupancy:?}");
    };
    assert_eq!(
        forecast.relief_ms, expected_ms,
        "{because}: expected relief at {expected_ms}ms"
    );
}

/// A machine the work cannot land on says nothing about when the work can
/// start.
///
/// The specimen: a Linux-targeted check whose only eligible machine is busy
/// for five minutes, beside a colocated macOS machine that frees in four
/// seconds. Scoping the forecast by mobility alone would hand that check a
/// four-second reading, clamp its wait to the floor, surface it as
/// unavailable while its real blocker was still finite, and name the macOS
/// work on the row — which is the whole failure this policy exists to end.
#[test]
fn a_forecast_ignores_machines_the_work_could_never_land_on() {
    let pool = occupied_fleet();
    let repository = forecast_repository();
    let scope = |selector| forecast_scope(&repository, selector, PlacementMobility::SpillEligible);
    let linux = ExecutorSelector {
        os: Some("linux".into()),
        ..ExecutorSelector::default()
    };
    assert_relief_near(
        &pool.occupancy_for_at(scope(Some(&linux)), NOW),
        300_000,
        "only the machine that can run it speaks for it",
    );
    assert_relief_near(
        &pool.occupancy_for_at(scope(None), NOW),
        4_000,
        "an unconstrained batch is genuinely relieved by whichever frees first",
    );

    let named = ExecutorSelector {
        name: Some("bglab-ub".into()),
        ..ExecutorSelector::default()
    };
    assert_relief_near(
        &pool.occupancy_for_at(scope(Some(&named)), NOW),
        300_000,
        "naming a machine is asking about that machine",
    );

    let unsatisfiable = ExecutorSelector {
        required_toolchains: vec!["matlab".into()],
        ..ExecutorSelector::default()
    };
    assert_eq!(
        pool.occupancy_for_at(scope(Some(&unsatisfiable)), NOW),
        occupancy::MachineOccupancy::Unforecastable,
        "no eligible machine is no knowledge, never an accidental prediction"
    );
}

/// Selector matching is not eligibility, and reading it as though it were is
/// the same bug in a costume.
///
/// Here the four-second machine satisfies every selector the work could
/// state and is still ineligible: it does not serve this project, or its
/// link has closed. Both are facts `candidate_rejection` knows and a
/// selector check cannot see, and either one alone would have the forecast
/// predict relief from a machine the work will never reach — clamping the
/// wait to the floor and printing that machine's occupant on the row.
/// Two remote machines that satisfy every selector this work could state:
/// `fast` frees in four seconds, `slow` in five minutes.
fn two_remotes() -> Fleet {
    let pool = Fleet::default();
    for (id, occupant, relief_ms) in [
        ("fast", executing("rust-fmt", 4_000), 4_000),
        ("slow", executing("rust-tests", 300_000), 300_000),
    ] {
        let (executor_id, mut connection) = fleet_entry(id, "linux", 0, &[]);
        connection.colocated = false;
        Arc::make_mut(&mut connection.snapshot).executing_requests = vec![occupant];
        Arc::make_mut(&mut connection.health).predicted_relief_ms = Some(relief_ms);
        pool.lock_connections().insert(executor_id, connection);
    }
    pool
}

/// Selector matching is not eligibility, and reading it as though it were is
/// the same bug in a costume.
///
/// Here the four-second machine satisfies every selector the work could
/// state and is still ineligible: it does not serve this project, or its
/// link has closed. Both are facts `candidate_rejection` knows and a
/// selector check cannot see, and either one alone would have the forecast
/// predict relief from a machine the work will never reach — clamping the
/// wait to the floor and printing that machine's occupant on the row.
/// A machine that can take no check never moves the capability identity a
/// check lane is keyed by, so enrolling one re-arms nothing.
#[test]
fn an_inference_only_machine_does_not_move_the_check_capability_identity() {
    let repository = forecast_repository();
    let scope = forecast_scope(&repository, None, PlacementMobility::SpillEligible);
    // Stamped with the real clock, which is what this identity judges
    // link freshness against.
    let now = unix_time_ms();
    let pool = Fleet::default();
    let (id, builder) = fleet_entry_at("fedora", "linux", 0, &[], now);
    pool.lock_connections().insert(id, builder);
    let before = pool.capability_identity(scope, &HashMap::new());
    assert_ne!(
        before,
        Fleet::default().capability_identity(scope, &HashMap::new()),
        "the builder is eligible, so the identity names it"
    );
    let (id, mut spark) = fleet_entry_at("sp1", "linux", 0, &[], now);
    spark.machine_role = MachineRole::InferenceOnly;
    pool.lock_connections().insert(id, spark);
    assert_eq!(pool.capability_identity(scope, &HashMap::new()), before);
    pool.hydrate_machine_roles(Vec::new());
    assert_ne!(
        pool.capability_identity(scope, &HashMap::new()),
        before,
        "the same machine as general is eligible, so the identity moves"
    );
}

/// cairn/5671. A machine whose telemetry goes stale stays eligible, so the
/// capability set alone cannot tell a blind window from its end, and a
/// lane suppressed during one outlived it. The identity names the
/// blindness: the counter a blind window charges is not the one the
/// recovered fleet reads, so the machine reporting again is itself the
/// fleet change that re-arms the lane, with no operator in the loop.
#[test]
fn a_machine_recovering_its_telemetry_moves_the_check_capability_identity() {
    const GIB: u64 = 1024 * 1024 * 1024;
    let repository = forecast_repository();
    let scope = forecast_scope(&repository, None, PlacementMobility::SpillEligible);
    let now = unix_time_ms();
    let pool = Fleet::default();
    let (id, mut fedora) = fleet_entry_at("fedora", "linux", 0, &[], now);
    measured_at(&mut fedora, 0.1, 16 * GIB, 512 * GIB, now);
    pool.lock_connections().insert(id.clone(), fedora);
    let seeing = pool.capability_identity(scope, &HashMap::new());
    assert!(
        pool.measurement_blindness(std::slice::from_ref(&id))
            .is_empty(),
        "fresh readings are not blind"
    );

    // The 10-31 specimen: the refresh pass wedged, so the last reading
    // ages past its bound while the link stays up.
    let stale_at = now - EXECUTOR_TELEMETRY_STALE_AFTER_MS - 18_000;
    measured_at(
        pool.lock_connections().get_mut(&id).unwrap(),
        0.1,
        16 * GIB,
        512 * GIB,
        stale_at,
    );
    let blind = pool.capability_identity(scope, &HashMap::new());
    assert_ne!(
        blind, seeing,
        "a blind machine is a different fleet from the same machine seeing"
    );
    let blindness = pool.measurement_blindness(std::slice::from_ref(&id));
    assert_eq!(blindness.len(), 1);
    assert_eq!(blindness[0].name, "fedora");
    assert!(
        blindness[0].reason.contains("past the"),
        "placement's own words: {}",
        blindness[0].reason
    );
    let blind_for = blindness[0]
        .blind_for_ms
        .expect("a stale reading is dated, so its blindness has a length");
    assert!(
        (18_000..120_000).contains(&blind_for),
        "measured from the bound, not from the reading: {blind_for}"
    );
    assert!(
        pool.measurement_blindness(&["gone".to_string()]).is_empty(),
        "a machine no longer attached is gone, not blind"
    );

    measured_at(
        pool.lock_connections().get_mut(&id).unwrap(),
        0.1,
        16 * GIB,
        512 * GIB,
        unix_time_ms(),
    );
    assert_eq!(
        pool.capability_identity(scope, &HashMap::new()),
        seeing,
        "fresh telemetry returns the fleet to the identity it had before the window"
    );
}

#[tokio::test]
async fn stored_fleet_suppression_rearms_on_telemetry_recovery_and_declared_repair() {
    use crate::execution::cache::{
        get_suppressed_check_result,
        tests::{die_against_fleet, fleet_scopes},
    };
    const GIB: u64 = 1 << 30;
    let repository = forecast_repository();
    let scope = forecast_scope(&repository, None, PlacementMobility::SpillEligible);
    for repair in [false, true] {
        let db = crate::execution::cache::tests::cache_db().await;
        let pool = Fleet::default();
        let now = unix_time_ms();
        let (id, mut entry) = fleet_entry_at("fedora", "linux", 0, &[], now);
        measured_at(
            &mut entry,
            0.05,
            48 * GIB,
            900 * GIB,
            if repair {
                now
            } else {
                now - EXECUTOR_TELEMETRY_STALE_AFTER_MS - 1_000
            },
        );
        pool.lock_connections().insert(id.clone(), entry);
        let old_digest = pool.capability_identity(scope, &HashMap::new());
        for commit in ["a", "b", "c"] {
            assert!(die_against_fleet(
                &db,
                "typecheck-ui",
                "unchanged-input",
                commit,
                &old_digest,
                false,
            ));
        }
        let suppressed = |digest: &str| {
            get_suppressed_check_result(
                db.clone(),
                "project-a",
                "typecheck-ui",
                "unchanged-input",
                &fleet_scopes("c", digest),
            )
            .unwrap()
            .is_some()
        };
        assert!(
            suppressed(&old_digest),
            "the unchanged-input lane is actually suppressed"
        );
        let epochs = if repair {
            super::repair::mark_repaired(&db, &id).await.unwrap();
            super::repair::repair_epochs(&db).await
        } else {
            measured_at(
                pool.lock_connections().get_mut(&id).unwrap(),
                0.05,
                48 * GIB,
                900 * GIB,
                unix_time_ms(),
            );
            HashMap::new()
        };
        let recovered_digest = pool.capability_identity(scope, &epochs);
        assert!(
            !suppressed(&recovered_digest),
            "recovery re-arms the same input on the same head"
        );
        assert!(
            suppressed(&old_digest),
            "recovery does not erase historical evidence"
        );
    }
}

#[test]
fn ending_an_update_hold_moves_the_fleet_suppression_identity() {
    let repository = forecast_repository();
    let scope = forecast_scope(&repository, None, PlacementMobility::SpillEligible);
    let now = unix_time_ms();
    let pool = Fleet::default();
    let (id, mut entry) = fleet_entry_at("fedora", "linux", 0, &[], now);
    runtime_update_hold_fixture(&mut entry, true);
    pool.lock_connections().insert(id.clone(), entry);
    let held = pool.capability_identity(scope, &HashMap::new());
    runtime_update_hold_fixture(pool.lock_connections().get_mut(&id).unwrap(), false);
    assert_ne!(
        pool.capability_identity(scope, &HashMap::new()),
        held,
        "returning from an update restores an eligible machine and re-arms its Fleet scopes"
    );
}

#[test]
fn a_repair_moves_only_capability_scopes_that_include_the_machine() {
    let repository = forecast_repository();
    let selector = ExecutorSelector {
        name: Some("fedora".into()),
        ..ExecutorSelector::default()
    };
    let scope = forecast_scope(
        &repository,
        Some(&selector),
        PlacementMobility::SpillEligible,
    );
    let now = unix_time_ms();
    let pool = Fleet::default();
    for name in ["fedora", "bbs"] {
        let (id, entry) = fleet_entry_at(name, "linux", 0, &[], now);
        pool.lock_connections().insert(id, entry);
    }
    let before = pool.capability_identity(scope, &HashMap::new());
    assert_eq!(
        pool.capability_identity(scope, &HashMap::from([("bbs".into(), 1)])),
        before,
        "a repair outside the selector is not evidence about this lane"
    );
    assert_ne!(
        pool.capability_identity(scope, &HashMap::from([("fedora".into(), 1)])),
        before,
        "an eligible machine's declared repair gives its fleet lanes a fresh budget"
    );
}

#[test]
fn a_forecast_reads_only_machines_placement_itself_would_keep() {
    let repository = forecast_repository();
    let scope = forecast_scope(&repository, None, PlacementMobility::SpillEligible);

    assert_relief_near(
        &two_remotes().occupancy_for_at(scope, NOW),
        4_000,
        "with both eligible, the sooner relief is the honest answer",
    );

    let unserved = two_remotes();
    Arc::make_mut(
        &mut unserved
            .lock_connections()
            .get_mut("fast")
            .unwrap()
            .advertisement,
    )
    .capabilities
    .projects_served = vec!["another-project".into()];
    assert_relief_near(
        &unserved.occupancy_for_at(scope, NOW),
        300_000,
        "a machine that does not serve this project cannot relieve it",
    );

    let disconnected = two_remotes();
    {
        let mut connections = disconnected.lock_connections();
        let (sender, receiver) = mpsc::unbounded_channel();
        drop(receiver);
        connections.get_mut("fast").unwrap().sender = sender;
    }
    assert_relief_near(
        &disconnected.occupancy_for_at(scope, NOW),
        300_000,
        "a machine whose link has closed cannot relieve anything",
    );
}

/// Mobility follows placement's own rule, which is about being UNtargeted.
///
/// A pinned-or-colocated batch that named nothing stays home, so only the
/// colocated machine speaks for it. The same batch that named a machine has
/// said where it goes, and placement lets it leave home — so the forecast
/// must follow it there rather than answering about a machine it is no
/// longer headed for.
#[test]
fn a_colocated_default_binds_untargeted_work_and_releases_targeted_work() {
    let pool = occupied_fleet();
    let repository = forecast_repository();
    assert_relief_near(
        &pool.occupancy_for_at(
            forecast_scope(&repository, None, PlacementMobility::PinnedOrColocated),
            NOW,
        ),
        4_000,
        "untargeted conservative work stays on the machine holding the checkout",
    );

    let named = ExecutorSelector {
        name: Some("bglab-ub".into()),
        ..ExecutorSelector::default()
    };
    assert_relief_near(
        &pool.occupancy_for_at(
            forecast_scope(
                &repository,
                Some(&named),
                PlacementMobility::PinnedOrColocated,
            ),
            NOW,
        ),
        300_000,
        "naming a machine is the permission to leave home, for placement and forecast alike",
    );
}

/// A checkout that exists on exactly one machine cannot be recreated
/// elsewhere, so no remote machine's occupancy speaks for work that carries
/// one — however mobile the batch declared itself.
#[test]
fn a_forecast_ignores_remotes_for_work_whose_checkout_cannot_travel() {
    let repository = RepositoryLocator::ExistingCheckout {
        project_id: FORECAST_PROJECT.into(),
        repository_id: FORECAST_PROJECT.into(),
        absolute_path: "/repo".into(),
    };
    assert_relief_near(
        &occupied_fleet().occupancy_for_at(
            forecast_scope(&repository, None, PlacementMobility::SpillEligible),
            NOW,
        ),
        4_000,
        "a checkout that cannot travel keeps the forecast at home",
    );
}

#[test]
fn disconnect_is_generation_fenced_for_health_invalidation() {
    let pool = Fleet::default();
    let (executor_id, connection) = fleet_entry("disconnect", "macos", 0, &[]);
    pool.lock_connections()
        .insert(executor_id.clone(), connection);
    assert!(!pool.disconnect_advertised_executor(&executor_id, 2));
    assert!(pool.disconnect_advertised_executor(&executor_id, 1));
    assert!(!pool.disconnect_advertised_executor(&executor_id, 1));
}

/// A machine's load history is the runner's, kept by executor identity, so
/// a link that drops and comes back extends the graph rather than
/// restarting it, and a frame from the replaced link adds nothing.
#[test]
fn a_machines_load_ring_survives_its_link_reconnecting() {
    let pool = Fleet::default();
    let (executor_id, connection) = fleet_entry("reconnecting", "linux", 0, &[]);
    pool.lock_connections()
        .insert(executor_id.clone(), connection);
    for tick in 1..=3 {
        let event = pool
            .record_machine_sample(&executor_id, 1, load::tests::sample_at(tick * 5_000, 0.2))
            .expect("a sample from the current link");
        assert_eq!(
            event.point.map(|point| point.at_unix_ms),
            Some(tick * 5_000)
        );
    }
    assert!(pool.disconnect_advertised_executor(&executor_id, 1));
    assert!(
        pool.record_machine_sample(&executor_id, 1, load::tests::sample_at(20_000, 0.2))
            .is_none(),
        "a frame from the link that went away describes nothing current"
    );

    let (_, mut reconnected) = fleet_entry("reconnecting", "linux", 0, &[]);
    reconnected.generation = 2;
    pool.lock_connections()
        .insert(executor_id.clone(), reconnected);
    pool.record_machine_sample(&executor_id, 2, load::tests::sample_at(25_000, 0.6))
        .expect("a sample from the new link");
    let series = &pool.fleet_load()[&executor_id];
    assert_eq!(
        series
            .points
            .iter()
            .map(|point| point.at_unix_ms)
            .collect::<Vec<_>>(),
        vec![5_000, 10_000, 15_000, 25_000],
        "the history from before the reconnect is still there"
    );
}

fn fleet_residency_request(holder: ResidencyHolder) -> ResidencyAcquireRequest {
    ResidencyAcquireRequest {
        holder,
        executor: None,
        owner_ref: None,
        selector: None,
        repository: RepositoryLocator::ColocatedPath {
            project_id: "p".into(),
            repository_id: "repo".into(),
            absolute_path: "/repo".into(),
        },
        initial_base_commit: "base".into(),
        footprint: cairn_common::executor_protocol::ResidencyFootprint::default(),
        death_policy: cairn_common::executor_protocol::OwnerDeathPolicy {
            heartbeat_timeout_ms: 30_000,
            reclaim_grace_ms: 10_000,
        },
        priority: CellPriority::AgentInteractive,
        wait_horizon_unix_ms: 1,
        waiting_since_unix_ms: 0,
    }
}

fn dev_instance_request(instance_id: &str) -> ResidencyAcquireRequest {
    fleet_residency_request(ResidencyHolder::DevInstance {
        instance_id: instance_id.into(),
    })
}

fn fleet_residency(
    holder: ResidencyHolder,
    owner_ref: Option<cairn_common::executor_protocol::CellOwnerRef>,
) -> CellResidency {
    let request = fleet_residency_request(holder.clone());
    CellResidency {
        holder,
        repository: request.repository,
        owner_ref,
        selector: None,
        incarnation_id: "incarnation".into(),
        current_base_commit: "base".into(),
        phase: cairn_common::executor_protocol::ResidencyPhase::Active,
        last_heartbeat_unix_ms: 1,
        reclaim_deadline_unix_ms: 0,
        death_policy: request.death_policy,
        footprint: request.footprint,
        state_revision: 1,
        events: Vec::new(),
    }
}

fn materialization_read_cell(
    cell_id: &str,
    job_id: &str,
) -> cairn_common::executor_protocol::PersistentCellState {
    let residency = fleet_residency(
        ResidencyHolder::Job {
            job_id: job_id.into(),
        },
        Some(cairn_common::executor_protocol::CellOwnerRef {
            project_id: "p".into(),
            project_key: Some("cairn".into()),
            issue_number: Some(1),
            job_id: Some(job_id.into()),
            execution_seq: Some(1),
            node_kind: Some("builder".into()),
        }),
    );
    cairn_common::executor_protocol::PersistentCellState {
        observed_head: None,
        size_bytes: 0,
        warm_command_classes: Vec::new(),
        executor_id: String::new(),
        executor_display_name: None,
        project_id: "p".into(),
        cell_id: cell_id.into(),
        path: format!("/cells/{cell_id}"),
        workspace_name: cell_id.into(),
        repository: "repo".into(),
        checkout_kind: Default::default(),
        git_common_dir: None,
        authority_path: format!("/authority/{cell_id}"),
        lifecycle: cairn_common::executor_protocol::PersistentCellLifecycle::Active,
        cell_epoch: 7,
        last_sealed_commit: Some("base".into()),
        last_distance: None,
        quarantine_reason: None,
        last_used_unix_ms: 1,
        last_affinity_key: None,
        preparation_fingerprint: Some("generation".into()),
        residency: Some(CellResidency {
            incarnation_id: format!("incarnation-{cell_id}"),
            ..residency
        }),
        occupancy: CellOccupancy::default(),
        seeded_from: None,
    }
}

#[test]
fn materialization_read_selection_is_stable_across_snapshot_order() {
    let repository = dev_instance_request("identity").repository.identity();
    let select = |reverse: bool| {
        let pool = Fleet::default();
        let mut connection = fleet_entry("executor", "macos", 0, &[]).1;
        Arc::make_mut(&mut connection.snapshot).cells = vec![
            materialization_read_cell("cell-b", "job"),
            materialization_read_cell("cell-a", "job"),
        ];
        if reverse {
            Arc::make_mut(&mut connection.snapshot).cells.reverse();
        }
        pool.lock_connections()
            .insert("executor".into(), connection);
        pool.select_materialization_read_candidate("run", "job", "p", &repository, "base")
            .unwrap()
            .cell_id
    };
    assert_eq!(select(false), "cell-a");
    assert_eq!(select(true), "cell-a");
}

#[test]
fn materialization_read_selection_rejects_owner_mismatch() {
    let pool = Fleet::default();
    let mut connection = fleet_entry("executor", "macos", 0, &[]).1;
    Arc::make_mut(&mut connection.snapshot).cells =
        vec![materialization_read_cell("cell", "different-job")];
    let repository = dev_instance_request("identity").repository.identity();
    pool.lock_connections()
        .insert("executor".into(), connection);
    assert!(matches!(
        pool.select_materialization_read_candidate("run", "job", "p", &repository, "base"),
        Err(MaterializationReadFailureKind::NoActiveMaterializationLease)
    ));
}

/// Another project's cell with the same ID, listed first in the same
/// executor's snapshot. Cell IDs are numbered per project, so this is an
/// ordinary snapshot on any executor serving two projects.
fn same_id_cell_in_other_project(
    held_by: Option<&str>,
) -> cairn_common::executor_protocol::PersistentCellState {
    let mut cell = materialization_read_cell("slot-1", held_by.unwrap_or("idle"));
    cell.project_id = "other".into();
    match held_by {
        Some(_) => {
            cell.residency.as_mut().unwrap().incarnation_id = "incarnation-other".into();
        }
        None => {
            cell.lifecycle = cairn_common::executor_protocol::PersistentCellLifecycle::Idle;
            cell.residency = None;
        }
    }
    cell
}

fn select_with_colliding_cell(
    colliding: cairn_common::executor_protocol::PersistentCellState,
) -> Result<MaterializationReadCandidate, MaterializationReadFailureKind> {
    let repository = dev_instance_request("identity").repository.identity();
    let pool = Fleet::default();
    let mut connection = fleet_entry("executor", "macos", 0, &[]).1;
    Arc::make_mut(&mut connection.snapshot).cells =
        vec![colliding, materialization_read_cell("slot-1", "job")];
    pool.lock_connections()
        .insert("executor".into(), connection);
    pool.select_materialization_read_candidate("run", "job", "p", &repository, "base")
}

fn expected_fence() -> ResidencyFence {
    ResidencyFence {
        holder: ResidencyHolder::Job {
            job_id: "job".into(),
        },
        incarnation_id: "incarnation-slot-1".into(),
        cell_epoch: 7,
    }
}

#[test]
fn materialization_read_survives_an_idle_cell_sharing_its_id_in_another_project() {
    let candidate = select_with_colliding_cell(same_id_cell_in_other_project(None)).unwrap();
    assert_eq!(candidate.cell_id, "slot-1");
    assert_eq!(candidate.fence, expected_fence());
}

#[test]
fn materialization_read_fences_its_own_holder_not_another_projects() {
    let candidate =
        select_with_colliding_cell(same_id_cell_in_other_project(Some("other-job"))).unwrap();
    assert_eq!(candidate.fence, expected_fence());
}

#[test]
fn a_panic_holding_the_connection_table_leaves_the_fleet_serving() {
    let pool = Fleet::default();
    let (executor_id, connection) = fleet_entry("executor", "macos", 0, &[]);
    let generation = connection.generation;
    pool.lock_connections()
        .insert(executor_id.clone(), connection);
    let panicked = std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let _held = pool.lock_connections();
                panic!("a request panicked while holding the connection table");
            })
            .join()
    });
    assert!(panicked.is_err());
    assert!(pool.connections.is_poisoned());

    // The path whose failure closed every executor link.
    let snapshot = FleetSnapshot {
        cells: vec![materialization_read_cell("slot-1", "job")],
        ..FleetSnapshot::default()
    };
    pool.set_executor_snapshot(
        &executor_id,
        generation,
        snapshot,
        ExecutorSubstrateReport::default(),
    );
    assert!(!pool.connections.is_poisoned());
    assert_eq!(pool.snapshot().cells.len(), 1);
    assert_eq!(pool.live_generation(&executor_id), Some(generation));
}

#[test]
fn lifetime_route_authority_recovers_after_transient_persistence_failure() {
    let temp = tempfile::tempdir().unwrap();
    let blocked_parent = temp.path().join("blocked");
    std::fs::write(&blocked_parent, "not a directory").unwrap();
    let route_path = blocked_parent.join("lifetime-routes.json");
    let pool = Fleet::with_residency_route_path(route_path.clone());
    let route = ResidencyRoute {
        holder: ResidencyHolder::DevInstance {
            instance_id: "launcher".into(),
        },
        repository: dev_instance_request("launcher").repository,
        executor_id: "first".into(),
        pending: true,
    };

    let initial_error = pool.reserve_pending_residency_route(route).unwrap_err();
    assert!(initial_error.contains("residency route authority"));
    let mut request = dev_instance_request("new-instance");
    assert!(matches!(
        pool.resolve_residency_acquire_route(&mut request),
        Err(ResidencyResult::Failed {
            kind: ResidencyFailureKind::Persistence,
            ..
        })
    ));

    std::fs::remove_file(&blocked_parent).unwrap();
    std::fs::create_dir(&blocked_parent).unwrap();

    assert!(pool
        .resolve_residency_acquire_route(&mut request)
        .unwrap()
        .is_none());
    assert!(route_path.is_file());
    assert!(pool.residency_route_store_error.lock().unwrap().is_none());
}

#[test]
fn fleet_lifetime_retry_routes_to_original_executor() {
    let pool = Fleet::default();
    let first = answering_fleet_entry("first", "linux", 10, &[]);
    let second = answering_fleet_entry("second", "linux", 0, &[]);
    pool.lock_connections().extend([first, second]);
    let mut request = dev_instance_request("launcher");
    // The executor holds the same repository by identity, addressed as
    // managed objects because it is not colocated.
    let persisted_repository = RepositoryLocator::ManagedObjects {
        project_id: "p".into(),
        repository_id: "repo".into(),
        object_format: GitObjectFormat::Sha1,
    };
    pool.residency_routes.lock().unwrap().insert(
        ("first".into(), request.holder.storage_key()),
        ResidencyRoute {
            holder: request.holder.clone(),
            repository: persisted_repository.clone(),
            executor_id: "first".into(),
            pending: false,
        },
    );
    let selected = pool
        .resolve_residency_acquire_route(&mut request)
        .unwrap()
        .unwrap();
    assert_eq!(selected.executor_id, "first");
    assert_eq!(request.repository, persisted_repository);
}

#[tokio::test]
async fn executor_restart_reconfigures_a_reacquired_live_residency() {
    let temp = tempfile::tempdir().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    let repository_path = temp.path().join("repository");
    std::fs::create_dir_all(&repository_path).unwrap();
    orch.db
        .local
        .execute_script(format!(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) \
                 VALUES ('p', 'default', 'Project', 'p', '{}', 1, 1);",
            repository_path.to_string_lossy()
        ))
        .await
        .unwrap();
    let holder = ResidencyHolder::Job {
        job_id: "survives-restart".into(),
    };
    let repository = RepositoryLocator::ColocatedPath {
        project_id: "p".into(),
        repository_id: "p".into(),
        absolute_path: repository_path.to_string_lossy().into_owned(),
    };
    let stale_incoming_repository = RepositoryLocator::ExistingCheckout {
        project_id: "p".into(),
        repository_id: "p".into(),
        absolute_path: "/stale/caller/coordinate".into(),
    };
    assert_eq!(repository.identity(), stale_incoming_repository.identity());

    let (first_sender, first_executor) = mpsc::unbounded_channel();
    let first_generation = orch.fleet.attach_executor(first_sender);
    orch.fleet.residency_routes.lock().unwrap().insert(
        (COLOCATED_EXECUTOR_ID.into(), holder.storage_key()),
        ResidencyRoute {
            holder: holder.clone(),
            repository: repository.clone(),
            executor_id: COLOCATED_EXECUTOR_ID.into(),
            pending: false,
        },
    );
    drop(first_executor);

    let (replacement_sender, mut replacement_executor) = mpsc::unbounded_channel();
    let replacement_generation = orch.fleet.attach_executor(replacement_sender);
    assert!(replacement_generation > first_generation);

    let mut cell = materialization_read_cell("resident-cell", "survives-restart");
    cell.project_id = "p".into();
    cell.residency.as_mut().unwrap().repository = repository.clone();
    assert!(orch.fleet.set_executor_snapshot(
        COLOCATED_EXECUTOR_ID,
        replacement_generation,
        FleetSnapshot {
            cells: vec![cell.clone()],
            ..FleetSnapshot::default()
        },
        ExecutorSubstrateReport::default(),
    ));
    let answering_fleet = orch.fleet.clone();
    let answer = tokio::spawn(async move {
        // Attach states the runner's drain posture before any work arrives,
        // so the re-acquire is not necessarily the first frame. Skipping
        // what attach says keeps this test about atomicity of config with
        // re-acquire rather than about frame position.
        let (correlation_id, config) = loop {
            match replacement_executor.recv().await {
                Some(ExecutorMessage::DrainModeRequest { .. }) => continue,
                Some(ExecutorMessage::ResidencyRequest {
                    correlation_id,
                    config: ResidencyRuntimeConfig::Install(config),
                    operation: ResidencyOperation::Acquire { .. },
                    granted_entry_id: None,
                }) => break (correlation_id, config),
                other => panic!(
                    "a restarted executor must receive config atomically with re-acquire, got {other:?}"
                ),
            }
        };
        assert_eq!(config.project_id, "p");
        assert_eq!(config.project_key, "p");
        answering_fleet.handle_executor_message(
            COLOCATED_EXECUTOR_ID,
            replacement_generation,
            ExecutorMessage::ResidencyResponse {
                correlation_id,
                result: ResidencyResult::State { cell },
            },
        );
    });

    let mut request = fleet_residency_request(holder);
    request.repository = stale_incoming_repository;
    request.wait_horizon_unix_ms = unix_time_ms().saturating_add(5_000);
    let result = orch
        .fleet
        .operate_residency(&orch, ResidencyOperation::Acquire { request })
        .await;
    answer.await.unwrap();
    assert!(matches!(result, ResidencyResult::State { .. }));
}

/// The incident end to end at the fleet's own entry point: a job whose
/// environment was on a machine that is no longer connected acquires again,
/// lands on a live machine, and leaves the route authority naming only that
/// machine. Before this, the same call returned `Admission` — "the executor
/// holding this execution environment is disconnected" — on every batch for
/// the rest of the job's life (CAIRN-4332).
#[tokio::test]
async fn a_job_whose_machine_is_gone_acquires_again_on_a_live_one() {
    let temp = tempfile::tempdir().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    let repository_path = temp.path().join("repository");
    std::fs::create_dir_all(&repository_path).unwrap();
    orch.db
        .local
        .execute_script(format!(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) \
                 VALUES ('p', 'default', 'Project', 'p', '{}', 1, 1);",
            repository_path.to_string_lossy()
        ))
        .await
        .unwrap();
    let holder = ResidencyHolder::Job {
        job_id: "stranded-by-a-departed-machine".into(),
    };
    let repository = RepositoryLocator::ColocatedPath {
        project_id: "p".into(),
        repository_id: "p".into(),
        absolute_path: repository_path.to_string_lossy().into_owned(),
    };
    orch.fleet.residency_routes.lock().unwrap().insert(
        ("departed".into(), holder.storage_key()),
        ResidencyRoute {
            holder: holder.clone(),
            repository: repository.clone(),
            executor_id: "departed".into(),
            pending: false,
        },
    );
    machine_left(&orch.fleet, "departed");

    let (sender, mut executor) = mpsc::unbounded_channel();
    let generation = orch.fleet.attach_announced_executor(sender);
    assert!(matches!(
        executor.recv().await,
        Some(ExecutorMessage::DrainModeRequest { enabled: false, .. })
    ));
    let mut cell = materialization_read_cell("rebuilt-cell", "stranded-by-a-departed-machine");
    cell.project_id = "p".into();
    cell.residency.as_mut().unwrap().repository = repository.clone();
    let answering_fleet = orch.fleet.clone();
    let answered = cell.clone();
    let answer = tokio::spawn(async move {
        let Some(ExecutorMessage::Offer { entry_id, .. }) = executor.recv().await else {
            panic!("a rebuilt environment must first be offered to the live executor");
        };
        answering_fleet.handle_executor_message(
            COLOCATED_EXECUTOR_ID,
            generation,
            ExecutorMessage::Claim {
                entry_id: entry_id.clone(),
            },
        );
        let Some(ExecutorMessage::ResidencyRequest {
            correlation_id,
            operation: ResidencyOperation::Acquire { .. },
            ..
        }) = executor.recv().await
        else {
            panic!("a rebuilt environment must be acquired on the live executor");
        };
        answering_fleet.handle_executor_message(
            COLOCATED_EXECUTOR_ID,
            generation,
            ExecutorMessage::ResidencyResponse {
                correlation_id,
                result: ResidencyResult::State { cell: answered },
            },
        );
    });

    let mut request = fleet_residency_request(holder.clone());
    request.repository = repository;
    request.wait_horizon_unix_ms = unix_time_ms().saturating_add(5_000);
    let result = orch
        .fleet
        .operate_residency(&orch, ResidencyOperation::Acquire { request })
        .await;
    answer.await.unwrap();

    assert!(matches!(result, ResidencyResult::State { .. }));
    let routes = orch.fleet.residency_routes.lock().unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(
        routes.values().next().unwrap().executor_id,
        COLOCATED_EXECUTOR_ID
    );
    drop(routes);
    assert!(matches!(
        orch.fleet.take_environment_eviction(&holder),
        Some(EnvironmentEviction {
            executor,
            cause: EvictionCause::Disconnected { .. },
            ..
        }) if executor == "departed"
    ));
}

#[tokio::test]
async fn executor_restart_configures_a_cell_in_the_submission_that_consumes_it() {
    let temp = tempfile::tempdir().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    let (first_sender, first_executor) = mpsc::unbounded_channel();
    let first_generation = orch.fleet.attach_executor(first_sender);
    drop(first_executor);

    let (replacement_sender, mut replacement_executor) = mpsc::unbounded_channel();
    let replacement_generation = orch.fleet.attach_announced_executor(replacement_sender);
    assert!(replacement_generation > first_generation);

    let answering_fleet = orch.fleet.clone();
    let answer = tokio::spawn(async move {
        // Attach states the runner's drain posture before any work is sent,
        // so the submission is not necessarily the first frame. Skipping
        // what attach says rather than asserting on frame position keeps
        // this test about the claim it makes — that a grant carries the
        // runtime config for the cell that consumes it — and keeps a future
        // attach frame from turning it red again.
        //
        // The config rides the grant rather than the offer because the grant
        // is what binds: an offer carries nothing a machine could act on, so
        // it could not carry this either.
        let (config, request) = loop {
            match replacement_executor.recv().await {
                Some(ExecutorMessage::DrainModeRequest { .. }) => continue,
                Some(ExecutorMessage::Offer { entry_id, .. }) => {
                    // Answer the offer the way a live machine would, so the
                    // runner reaches the grant this test is about.
                    answering_fleet.handle_executor_message(
                        COLOCATED_EXECUTOR_ID,
                        replacement_generation,
                        ExecutorMessage::Claim { entry_id },
                    );
                    continue;
                }
                Some(ExecutorMessage::Granted {
                    config,
                    request,
                    batch: None,
                    ..
                }) => break (config, request),
                other => {
                    panic!("a cell must carry runtime config in its grant, got {other:?}")
                }
            }
        };
        assert_eq!(config.project_id, request.project_id);
        answering_fleet.handle_executor_message(
            COLOCATED_EXECUTOR_ID,
            replacement_generation,
            ExecutorMessage::Result {
                request_id: request.request_id.clone(),
                attempt_id: request.attempt_id.clone(),
                outcome: CellOutcome::Cancelled {
                    request_id: request.request_id,
                    attempt_id: request.attempt_id,
                },
            },
        );
    });

    let mut request = provenance_request();
    request.repository = RepositoryLocator::ScratchOnly {
        owner_id: request.project_id.clone(),
    };
    let result = orch.fleet.submit(&orch, request).await;
    answer.await.unwrap();
    assert!(matches!(result, CellOutcome::Cancelled { .. }));
}

#[tokio::test]
async fn a_drain_between_claim_and_grant_releases_the_unstarted_machine_reservation() {
    let temp = tempfile::tempdir().unwrap();
    let orch = Arc::new(test_orchestrator(temp.path()).await);
    let (sender, mut remote) = mpsc::unbounded_channel();
    let (_, advertised) = fleet_entry("remote", "macos", 0, &[]);
    let generation = orch
        .fleet
        .attach_advertised_executor(
            Arc::unwrap_or_clone(advertised.advertisement),
            Default::default(),
            sender,
            false,
            None,
            None,
        )
        .unwrap();
    orch.fleet.announce_room_for_test("remote", generation);
    let mut request = provenance_request();
    request.repository = RepositoryLocator::ScratchOnly {
        owner_id: request.project_id.clone(),
    };
    request.pinned_executor_id = Some("remote".into());
    let submitting = {
        let orch = orch.clone();
        tokio::spawn(async move { orch.fleet.submit_execution(&orch, request, None).await })
    };
    let (offered, summary) = loop {
        match tokio::time::timeout(Duration::from_secs(30), remote.recv()).await {
            Ok(Some(ExecutorMessage::DrainModeRequest { .. })) => continue,
            Ok(Some(ExecutorMessage::Offer { entry_id, summary })) => break (entry_id, summary),
            other => panic!("expected an offer before the drain, got {other:?}"),
        }
    };
    let (_machine_home, machine) = claiming_machine();
    machine_claims(&machine, &offered, &summary).await;
    orch.fleet.handle_executor_message(
        "remote",
        generation,
        ExecutorMessage::Claim {
            entry_id: offered.clone(),
        },
    );
    // This current-thread test does not yield between accepting Claim and
    // publication, so the resumed dispatch must encounter the new fence.
    let fence =
        orch.fleet
            .fence_admission_owned("remote", "removal".into(), "claim-race fence".into());
    let outcome = tokio::time::timeout(Duration::from_secs(30), submitting)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(outcome, CellOutcome::Unavailable { ref diagnostic, .. }
        if diagnostic.contains("claim-race fence") && !diagnostic.contains("connection closed")),
        "{outcome:?}"
    );
    let mut lost = 0;
    while let Ok(frame) = remote.try_recv() {
        match frame {
            frame @ ExecutorMessage::DrainModeRequest { .. } => {
                machine_hears(&machine, frame).await
            }
            frame @ ExecutorMessage::Lost { .. } => {
                assert!(
                    matches!(&frame, ExecutorMessage::Lost { entry_id } if entry_id == &offered)
                );
                lost += 1;
                machine_hears(&machine, frame).await;
            }
            other => panic!("a vetoed claim receives no grant or cancel, got {other:?}"),
        }
    }
    assert_eq!(lost, 1);
    assert!(orch
        .fleet
        .queue
        .with_entry_by_identity(
            &(summary.request_id.clone(), summary.attempt_id.clone()),
            |_| (),
        )
        .is_none());
    assert!(orch.fleet.lift_admission_fence(&fence));
    match remote.try_recv() {
        Ok(frame @ ExecutorMessage::DrainModeRequest { enabled: false, .. }) => {
            machine_hears(&machine, frame).await
        }
        other => panic!("the executor receives the committed admission clear, got {other:?}"),
    }
    assert!(
        remote.try_recv().is_err(),
        "no duplicate Lost follows the clear"
    );
    assert_machine_released(&machine, &summary).await;
}

/// A real executor pool standing in for the machine on the other end of
/// the link, so a test observes the machine's own accounting rather than
/// asserting on the frames a scripted peer happened to receive.
fn claiming_machine() -> (tempfile::TempDir, cairn_exec::Fleet) {
    let home = tempfile::tempdir().unwrap();
    let machine =
        cairn_exec::Fleet::new(cairn_exec::ExecutorRuntime::new(home.path().join("home")));
    (home, machine)
}

/// Answer an offer the way the executor's link handler does: on the
/// blocking pool, where the handler runs.
async fn machine_claims(
    machine: &cairn_exec::Fleet,
    entry_id: &str,
    summary: &cairn_common::executor_protocol::OfferSummary,
) {
    let (machine, entry_id, summary) = (machine.clone(), entry_id.to_owned(), summary.clone());
    cairn_exec::blocking::offload(move || {
        machine
            .try_claim(&entry_id, &summary)
            .expect("an idle machine claims the offer");
        assert!(machine.bind_request_identity(&summary.request_id));
    })
    .await;
}

/// Apply a runner frame that withdraws or cancels work, the way the
/// executor's link handler does: on the blocking pool, where the handler
/// runs.
async fn machine_hears(machine: &cairn_exec::Fleet, frame: ExecutorMessage) {
    let machine = machine.clone();
    cairn_exec::blocking::offload(move || match frame {
        ExecutorMessage::DrainModeRequest { enabled, .. } => {
            machine.set_drain_mode(enabled);
        }
        ExecutorMessage::Lost { entry_id } => {
            if let Some(request_id) = machine.release_claim(&entry_id) {
                machine.release_request_identity(&request_id);
            }
        }
        ExecutorMessage::Cancel { request_id, .. } => {
            machine.cancel_request(&request_id);
        }
        other => panic!("not a withdrawal: {other:?}"),
    })
    .await;
}

/// The silence bound the granted-attempt tests run under, short enough that
/// a verdict arrives in well under a second.
const TEST_CLAIMED_SILENCE_MS: u64 = 300;

/// A batch the colocated machine has claimed and been granted, with the
/// runner waiting on its result. The machine has posted no snapshot since
/// the grant, so until a test posts one it has not accounted for the work.
async fn granted_and_held(
    orch: &Arc<Orchestrator>,
) -> (
    u64,
    tokio::task::JoinHandle<ExecutedRequest>,
    mpsc::UnboundedReceiver<ExecutorMessage>,
) {
    let (generation, submitting, mut machine, offered) = offered_and_held(orch).await;
    orch.fleet.handle_executor_message(
        COLOCATED_EXECUTOR_ID,
        generation,
        ExecutorMessage::Claim { entry_id: offered },
    );
    loop {
        match tokio::time::timeout(Duration::from_secs(30), machine.recv()).await {
            Ok(Some(ExecutorMessage::Granted { .. })) => break,
            Ok(Some(_)) => continue,
            other => panic!("the claimed work was never granted: {other:?}"),
        }
    }
    (generation, submitting, machine)
}

/// The same batch, offered to the colocated machine and not yet answered.
async fn offered_and_held(
    orch: &Arc<Orchestrator>,
) -> (
    u64,
    tokio::task::JoinHandle<ExecutedRequest>,
    mpsc::UnboundedReceiver<ExecutorMessage>,
    String,
) {
    let (sender, mut machine) = mpsc::unbounded_channel();
    let generation = orch.fleet.attach_announced_executor(sender);
    orch.fleet.claimed_attempt_silence.0.store(
        TEST_CLAIMED_SILENCE_MS,
        std::sync::atomic::Ordering::Relaxed,
    );

    let submitting = scratch_submission(orch).await;
    let offered = loop {
        match tokio::time::timeout(Duration::from_secs(30), machine.recv()).await {
            Ok(Some(ExecutorMessage::Offer { entry_id, .. })) => break entry_id,
            Ok(Some(_)) => continue,
            other => panic!("the runner never offered the work: {other:?}"),
        }
    };
    (generation, submitting, machine, offered)
}

/// Submit a scratch-only batch pinned to the colocated executor, as a
/// leader would, without waiting for its outcome.
async fn scratch_submission(orch: &Arc<Orchestrator>) -> tokio::task::JoinHandle<ExecutedRequest> {
    scratch_submission_with_stop(orch, None, true).await
}

async fn scratch_submission_with_stop(
    orch: &Arc<Orchestrator>,
    stop: Option<Arc<PlacementStop>>,
    resident_home: bool,
) -> tokio::task::JoinHandle<ExecutedRequest> {
    let mut request = provenance_request();
    request.repository = RepositoryLocator::ScratchOnly {
        owner_id: request.project_id.clone(),
    };
    if resident_home {
        request.pinned_executor_id = Some(COLOCATED_EXECUTOR_ID.into());
    } else {
        request.executor = Some(ExecutorSelector {
            name: Some(LOCAL_EXECUTOR_NAME.into()),
            ..ExecutorSelector::default()
        });
        assert!(matches!(
            candidate_rejection(
                &orch.fleet.lock_connections()[COLOCATED_EXECUTOR_ID],
                PlacementScope::of(&request),
                unix_time_ms(),
            ),
            Some(PlacementRejectionReason::UpdatePending { .. })
        ));
    }
    let batch = ProcessBatch {
        files: Vec::new(),
        runtime_packages: Vec::new(),
        sequential: true,
        stop_on_error: true,
        sandbox_mode: ProcessSandboxMode::Unconfined,
        timeout_bases: Vec::new(),
        items: vec![cache_batch_item(Vec::new())],
        runner_context_id: None,
        execution_residency: None,
        containment: None,
        node_scratch: None,
    };
    let prepared = orch
        .fleet
        .prepare_execution(orch, &mut request)
        .await
        .expect("a scratch request prepares");
    let orch = orch.clone();
    tokio::spawn(async move {
        orch.fleet
            .execute_prepared_accounted(request, Some(batch), prepared, stop.as_deref())
            .await
    })
}

async fn settled(submitting: tokio::task::JoinHandle<ExecutedRequest>) -> ExecutedRequest {
    tokio::time::timeout(Duration::from_secs(30), submitting)
        .await
        .expect("the silence bound ends the wait")
        .unwrap()
}

/// Route this fleet's queue journal to the test, so the terminal row each
/// entry leaves behind can be read.
fn journal_rows(orch: &Orchestrator) -> mpsc::UnboundedReceiver<queue::QueueJournalBatch> {
    let (sender, receiver) = mpsc::unbounded_channel();
    orch.fleet.queue.attach_journal(sender);
    receiver
}

/// The terminal row journaled for one request, once it has one.
fn terminal_row(
    journal: &mut mpsc::UnboundedReceiver<queue::QueueJournalBatch>,
    request_id: &str,
) -> queue::QueueJournalRow {
    let mut rows = Vec::new();
    while let Ok(batch) = journal.try_recv() {
        rows.extend(batch);
    }
    rows.into_iter()
        .rfind(|row| {
            row.request_id == request_id
                && matches!(row.state.as_str(), "completed" | "cancelled" | "expired")
        })
        .unwrap_or_else(|| panic!("no terminal row journaled for {request_id}"))
}

/// The cairn/4966 property, for every caller: dropping the future that
/// awaits a granted request is a cancellation. The machine running it is
/// told to stop, the entry leaves the queue at once with a row that says a
/// caller went away and where the request stood, and the machine's own
/// terminal answer, arriving after, is accepted harmlessly rather than
/// reviving anything. Nothing is left for a restart to find.
#[tokio::test]
async fn dropping_the_caller_of_a_running_request_cancels_it_and_says_why() {
    let temp = tempfile::tempdir().unwrap();
    let orch = Arc::new(test_orchestrator(temp.path()).await);
    let mut journal = journal_rows(&orch);
    let (generation, submitting, mut machine) = granted_and_held(&orch).await;
    let request = provenance_request();

    submitting.abort();
    match submitting.await {
        Err(error) => assert!(error.is_cancelled()),
        Ok(executed) => panic!(
            "the caller was dropped, not answered: {:?}",
            executed.outcome
        ),
    }

    let cancelled = loop {
        match tokio::time::timeout(Duration::from_secs(5), machine.recv()).await {
            Ok(Some(ExecutorMessage::Cancel { request_id, .. })) => break request_id,
            Ok(Some(_)) => continue,
            other => panic!("the machine running the work was never told: {other:?}"),
        }
    };
    assert_eq!(cancelled, request.request_id);
    assert_eq!(orch.fleet.queue.len(), 0, "nothing is left for a restart");
    let row = terminal_row(&mut journal, &request.request_id);
    assert_eq!(row.state, "cancelled");
    let detail = row.detail.expect("a withdrawal names its cause");
    assert!(
        detail.contains("the caller awaiting this request went away")
            && detail.contains("running on"),
        "{detail}"
    );

    // The executor ends the item and answers; the request is already
    // settled, so the answer finds nothing to settle and changes nothing.
    orch.fleet.handle_executor_message(
        COLOCATED_EXECUTOR_ID,
        generation,
        ExecutorMessage::Result {
            request_id: request.request_id.clone(),
            attempt_id: request.attempt_id.clone(),
            outcome: CellOutcome::Cancelled {
                request_id: request.request_id.clone(),
                attempt_id: request.attempt_id.clone(),
            },
        },
    );
    assert_eq!(orch.fleet.queue.len(), 0);
}

/// Work still being placed when the runner begins to stop is withdrawn
/// naming the stop. Before, it went on waiting, found the colocated machine
/// gone once the shutdown removed it, and was recorded as work no machine
/// could run.
#[tokio::test]
async fn runner_shutdown_withdraws_an_offered_request_naming_the_stop() {
    let temp = tempfile::tempdir().unwrap();
    let orch = Arc::new(test_orchestrator(temp.path()).await);
    let mut journal = journal_rows(&orch);
    let (_generation, submitting, _machine, _offered) = offered_and_held(&orch).await;

    orch.fleet.begin_runner_shutdown();
    let executed = settled(submitting).await;

    match &executed.outcome {
        CellOutcome::Unavailable {
            reason: CellUnavailableReason::RunnerStopping { .. },
            diagnostic,
            // The shutdown's own close of the colocated link returns the
            // unanswered offer to the queue before this request wakes, so the
            // phase it names is where it stood when it was withdrawn.
        } => assert!(
            diagnostic.contains("began shutting down")
                && diagnostic.contains("waiting to be placed"),
            "{diagnostic}"
        ),
        other => panic!("the stop is what this request is told: {other:?}"),
    }
    assert!(
        executed.grant_without_result.is_none(),
        "nothing was granted"
    );
    assert_eq!(orch.fleet.queue.len(), 0);
    let row = terminal_row(&mut journal, &provenance_request().request_id);
    assert_eq!(row.state, "cancelled");
    assert!(
        row.detail
            .as_deref()
            .is_some_and(|detail| detail.contains("began shutting down")),
        "{:?}",
        row.detail
    );
}

/// Work running on the colocated machine when the runner begins to stop is
/// answered with the stop, never with the closed connection the shutdown
/// produces on its way out.
#[tokio::test]
async fn runner_shutdown_ends_a_running_request_with_the_stop() {
    let temp = tempfile::tempdir().unwrap();
    let orch = Arc::new(test_orchestrator(temp.path()).await);
    let mut journal = journal_rows(&orch);
    let (_generation, submitting, _machine) = granted_and_held(&orch).await;

    orch.fleet.begin_runner_shutdown();
    let executed = settled(submitting).await;

    assert!(
        matches!(
            executed.outcome,
            CellOutcome::Unavailable {
                reason: CellUnavailableReason::RunnerStopping { .. },
                ..
            }
        ),
        "{:?}",
        executed.outcome
    );
    assert!(
        executed.grant_without_result.is_some(),
        "the batch was handed to a machine, so the run envelope may not say nothing ran"
    );
    let row = terminal_row(&mut journal, &provenance_request().request_id);
    let detail = row.detail.unwrap_or_default();
    assert!(detail.contains("began shutting down"), "{detail}");
    assert!(!detail.contains("connection closed"), "{detail}");
}

/// A stop that reaches a granted request before its machine answers (an
/// enrolled machine, whose link the shutdown leaves alone) ends the wait,
/// tells the machine to stop, and keeps the account of the grant, so a run
/// batch is reported as handed over with no result rather than as never
/// having run.
#[tokio::test]
async fn a_stop_during_the_grant_wait_keeps_the_grant_and_cancels_the_work() {
    let temp = tempfile::tempdir().unwrap();
    let orch = Arc::new(test_orchestrator(temp.path()).await);
    let mut journal = journal_rows(&orch);
    let (_generation, submitting, mut machine) = granted_and_held(&orch).await;

    orch.fleet.runner_stop.begin(unix_time_ms());
    let executed = settled(submitting).await;

    assert!(
        matches!(
            executed.outcome,
            CellOutcome::Unavailable {
                reason: CellUnavailableReason::RunnerStopping { .. },
                ..
            }
        ),
        "{:?}",
        executed.outcome
    );
    let grant = executed
        .grant_without_result
        .expect("the grant's account survives the stop");
    assert_eq!(
        grant.executor_name,
        orch.fleet.executor_name_for_text(COLOCATED_EXECUTOR_ID)
    );
    let cancelled = loop {
        match tokio::time::timeout(Duration::from_secs(5), machine.recv()).await {
            Ok(Some(ExecutorMessage::Cancel { request_id, .. })) => break request_id,
            Ok(Some(_)) => continue,
            other => panic!("the machine was never told to stop: {other:?}"),
        }
    };
    assert_eq!(cancelled, provenance_request().request_id);
    let detail = terminal_row(&mut journal, &provenance_request().request_id)
        .detail
        .unwrap_or_default();
    assert!(
        detail.contains("began shutting down") && detail.contains("running on"),
        "{detail}"
    );
}

/// A result the runner has already accepted is the answer, whatever
/// arrives after it. The machine's completion is settled into the queue
/// and the stop is recorded before the waiting task is polled at all; the
/// caller still receives the completion, and its bookkeeping runs.
#[tokio::test]
async fn a_result_accepted_before_the_stop_is_not_replaced_by_it() {
    let temp = tempfile::tempdir().unwrap();
    let orch = Arc::new(test_orchestrator(temp.path()).await);
    let (generation, submitting, _machine) = granted_and_held(&orch).await;
    let request = provenance_request();

    orch.fleet.handle_executor_message(
        COLOCATED_EXECUTOR_ID,
        generation,
        ExecutorMessage::Result {
            request_id: request.request_id.clone(),
            attempt_id: request.attempt_id.clone(),
            outcome: completed_for(&request, COLOCATED_EXECUTOR_ID, generation),
        },
    );
    orch.fleet.runner_stop.begin(unix_time_ms());
    let executed = settled(submitting).await;

    assert!(
        matches!(executed.outcome, CellOutcome::Completed { .. }),
        "{:?}",
        executed.outcome
    );
    assert!(executed.grant_without_result.is_none());
    assert_eq!(orch.fleet.queue.len(), 0);
}

/// A request that arrives after the stop never enters the queue: there is
/// no runner left to wait on its answer.
#[tokio::test]
async fn a_request_after_the_stop_is_refused_without_being_queued() {
    let temp = tempfile::tempdir().unwrap();
    let orch = Arc::new(test_orchestrator(temp.path()).await);
    let mut journal = journal_rows(&orch);
    orch.fleet.begin_runner_shutdown();

    let mut request = provenance_request();
    request.repository = RepositoryLocator::ScratchOnly {
        owner_id: request.project_id.clone(),
    };
    let prepared = orch
        .fleet
        .prepare_execution(&orch, &mut request)
        .await
        .expect("a scratch request prepares");
    let executed = orch
        .fleet
        .execute_prepared_accounted(request, None, prepared, None)
        .await;

    assert!(matches!(
        executed.outcome,
        CellOutcome::Unavailable {
            reason: CellUnavailableReason::RunnerStopping { .. },
            ..
        }
    ));
    assert_eq!(orch.fleet.queue.len(), 0);
    assert!(journal.try_recv().is_err(), "nothing was ever queued");
}

fn silent_accounts(
    outcome: &CellOutcome,
) -> Option<(Option<ClaimedAttemptAccount>, ClaimedAttemptAccount)> {
    match outcome {
        CellOutcome::Unavailable {
            reason:
                CellUnavailableReason::ClaimedAttemptSilent {
                    last_progress,
                    final_account,
                    ..
                },
            ..
        } => Some((*last_progress, *final_account)),
        _ => None,
    }
}

fn granted_snapshot(request: &CellRequest, executing: bool) -> FleetSnapshot {
    let mut snapshot = FleetSnapshot {
        granted_attempts: vec![cairn_common::executor_protocol::GrantedAttempt {
            request_id: request.request_id.clone(),
            attempt_id: request.attempt_id.clone(),
        }],
        ..FleetSnapshot::default()
    };
    if executing {
        let mut running = executing_fixture(&request.request_id);
        running.attempt_id = request.attempt_id.clone();
        snapshot.executing_requests = vec![running];
    }
    snapshot
}

fn executing_fixture(request_id: &str) -> ExecutingCellRequest {
    ExecutingCellRequest {
        executor_id: COLOCATED_EXECUTOR_ID.into(),
        cell_id: "cell".into(),
        request_id: request_id.into(),
        attempt_id: "attempt".into(),
        owner: None,
        command_class: Default::default(),
        command: "bun run test:rust".into(),
        started_at_unix_ms: unix_time_ms(),
        process_ids: vec![4242],
        priority: Some(CellPriority::ReviewCheck),
        subscriber_count: 1,
        resource_reservation: Default::default(),
        command_resource_identity: None,
        learned_estimate: None,
        live_usage: None,
        build_jobs: None,
        adopted: None,
        memory_phase: None,
        standalone: false,
    }
}

/// The self-update drain judges a running attempt by the stall-stop
/// window its own sample carried (cairn/5672), matched by attempt: a
/// window for another attempt of the same request, or none, leaves it
/// nothing to judge by.
#[test]
fn snapshot_work_carries_each_running_attempts_stall_window() {
    use cairn_common::executor_protocol::{
        LiveUsage, LiveUsageSample, LiveUsageSubject, MeasurementQuality, QuietWindow,
    };
    use cairn_common::wire_vocabulary::Measurement;

    let (_, mut entry) = fleet_entry("remote-a", "linux", 0, &[]);
    let mut judged = executing_fixture("r1");
    judged.attempt_id = "a1".into();
    let mut retried = executing_fixture("r2");
    retried.attempt_id = "a2".into();
    Arc::make_mut(&mut entry.snapshot).executing_requests = vec![judged, retried];
    let usage = |request_id: &str, attempt_id: &str| LiveUsage {
        subject: LiveUsageSubject::Command {
            request_id: request_id.into(),
            attempt_id: attempt_id.into(),
        },
        sample: LiveUsageSample {
            window_cpu_ms: 4_000,
            window_ms: 5_000,
            resident_bytes: None,
            measured_at_unix_ms: 40,
            quality: MeasurementQuality::Sampled,
            quiet_window: Some(QuietWindow {
                quiet_ms: 120_000,
                silence_bound_ms: Fleet::REVIEW_CHECK_SILENCE_BOUND_MS,
            }),
            scheduling: None,
        },
    };
    assert!(entry.live_usage.adopt(&Measurement::measured(
        40,
        vec![usage("r1", "a1"), usage("r2", "a1")],
    )));

    let (work, _, _) = snapshot_work(&entry);
    let progress: Vec<_> = work
        .iter()
        .map(|item| (item.key.clone(), item.progress))
        .collect();
    assert_eq!(
        progress,
        vec![
            (
                held_request_key("r1", "a1"),
                Some(rollout::Progress {
                    quiet_ms: 120_000,
                    silence_bound_ms: Fleet::REVIEW_CHECK_SILENCE_BOUND_MS,
                    measured_at_unix_ms: 40,
                })
            ),
            (held_request_key("r2", "a2"), None),
        ]
    );

    // Witnesses for one physical attempt union, but retries of one request
    // remain distinct even while an older result is retained.
    Arc::make_mut(&mut entry.snapshot).granted_attempts = vec![
        cairn_common::executor_protocol::GrantedAttempt {
            request_id: "r1".into(),
            attempt_id: "a1".into(),
        },
        cairn_common::executor_protocol::GrantedAttempt {
            request_id: "r1".into(),
            attempt_id: "a2".into(),
        },
    ];
    let (mut work, _, _) = snapshot_work(&entry);
    join_runner_work(
        &mut work,
        &[("r1".into(), "a2".into()), ("r1".into(), "a3".into())],
        &[],
    );
    assert_eq!(
        work.len(),
        4,
        "r1's three attempts and r2's attempt must not share a suppression identity"
    );
}

/// A terminal, a REPL and a dev instance are residents the restart ends,
/// named with whose they are and addressed for their owner's notice; a
/// long-lived service is cycled; a service's one-shot command is work.
#[test]
fn snapshot_work_names_residents_apart_from_work_and_services() {
    use cairn_common::executor_protocol::{
        ResidentProcess, ResidentProcessKind, ResidentProcessStatus,
    };
    let (_, mut entry) = fleet_entry("fedora", "linux", 0, &[]);
    let mut cell = materialization_read_cell("fedora-home", "builder");
    let holder = cell.residency.as_ref().unwrap().holder.clone();
    let running = |kind| ResidentProcess {
        generation: 1,
        kind,
        spec: None,
        status: ResidentProcessStatus::Running {
            started_at_unix_ms: 1,
            process_group_id: None,
            ownership: None,
        },
        reservation: None,
        live_usage: None,
        charge: None,
    };
    cell.occupancy.processes = std::collections::BTreeMap::from([
        (
            "terminal:lab".to_string(),
            running(ResidentProcessKind::Terminal { slug: "lab".into() }),
        ),
        (
            "repl:py".to_string(),
            running(ResidentProcessKind::Repl { slug: "py".into() }),
        ),
        (
            "dev".to_string(),
            running(ResidentProcessKind::DevInstance {
                source_terminal_session_id: None,
            }),
        ),
        (
            "watch".to_string(),
            running(ResidentProcessKind::Service {
                name: "iMessage channel".into(),
                role: "watch".into(),
            }),
        ),
        (
            "once".to_string(),
            running(ResidentProcessKind::Service {
                name: "setup".into(),
                role: service_placement::ONE_SHOT_ROLE.into(),
            }),
        ),
    ]);
    Arc::make_mut(&mut entry.snapshot).cells = vec![cell];

    let (work, services, residents) = snapshot_work(&entry);
    assert_eq!(
        work.iter()
            .map(|item| item.label.as_str())
            .collect::<Vec<_>>(),
        ["service `setup`"]
    );
    assert_eq!(services, ["service `iMessage channel`"]);
    assert_eq!(
        residents
            .iter()
            .map(|resident| resident.label.as_str())
            .collect::<Vec<_>>(),
        [
            "REPL `py` of cairn/1",
            "a dev instance of cairn/1",
            "terminal `lab` of cairn/1"
        ]
    );
    let lab = &residents[2];
    assert_eq!(lab.key, "process:fedora-home/terminal:lab/1");
    assert_eq!(lab.address.process_key, "terminal:lab");
    assert_eq!(lab.address.holder, Some(holder));
    assert_eq!(
        lab.address.kind,
        Some(ResidentProcessKind::Terminal { slug: "lab".into() })
    );
}

/// The cairn/4912 shape: a machine is granted a batch and is never heard
/// from about it again. The outcome the runner composes when the machine's
/// silence ends the wait has to arrive with the account of the grant, not
/// as a bare refusal a caller would read as "nothing ran".
#[tokio::test]
async fn a_grant_the_machine_never_answers_is_reported_as_unanswered() {
    let temp = tempfile::tempdir().unwrap();
    let orch = Arc::new(test_orchestrator(temp.path()).await);
    let mut journal = journal_rows(&orch);
    let (_generation, submitting, _machine) = granted_and_held(&orch).await;
    let executed = settled(submitting).await;

    assert_eq!(
        silent_accounts(&executed.outcome),
        Some((None, ClaimedAttemptAccount::NotListed)),
        "a connected machine that never listed the grant: {:?}",
        executed.outcome
    );
    let CellOutcome::Unavailable { diagnostic, .. } = &executed.outcome else {
        unreachable!()
    };
    assert!(
        diagnostic.contains("never listed the attempt after the grant"),
        "{diagnostic}"
    );
    // Ending the wait on the machine's silence is the runner's own
    // decision, and its journal row says so rather than blaming a caller
    // that is still waiting.
    let detail = terminal_row(&mut journal, &provenance_request().request_id)
        .detail
        .unwrap_or_default();
    assert!(
        detail.contains("gave no account") && !detail.contains("caller"),
        "{detail}"
    );
    let grant = executed
        .grant_without_result
        .expect("a grant the machine never answered is reported as one");
    assert_eq!(
        grant.executor_name,
        orch.fleet.executor_name_for_text(COLOCATED_EXECUTOR_ID)
    );
    assert_eq!(grant.connection_drops, Some(0));
}

/// The cairn/4981 specimen inverted: a claimed attempt its machine keeps
/// accounting for is waited on through many multiples of the silence
/// bound, across the held phase and the executing one, and ends with the
/// machine's own answer. No elapsed time since the grant ends it.
#[tokio::test]
async fn a_claimed_attempt_its_machine_keeps_accounting_for_is_never_ended_by_time() {
    let temp = tempfile::tempdir().unwrap();
    let orch = Arc::new(test_orchestrator(temp.path()).await);
    let (generation, submitting, _machine) = granted_and_held(&orch).await;
    let request = provenance_request();
    let pause = Duration::from_millis(TEST_CLAIMED_SILENCE_MS * 3);

    assert!(orch.fleet.set_executor_snapshot(
        COLOCATED_EXECUTOR_ID,
        generation,
        granted_snapshot(&request, false),
        ExecutorSubstrateReport::default(),
    ));
    tokio::time::sleep(pause).await;
    assert!(
        !submitting.is_finished(),
        "a held attempt (preparing its cell, between items) is progress"
    );
    orch.fleet.set_executor_snapshot(
        COLOCATED_EXECUTOR_ID,
        generation,
        granted_snapshot(&request, true),
        ExecutorSubstrateReport::default(),
    );
    tokio::time::sleep(pause).await;
    assert!(
        !submitting.is_finished(),
        "an executing attempt is progress, however long it runs"
    );

    orch.fleet.handle_executor_message(
        COLOCATED_EXECUTOR_ID,
        generation,
        ExecutorMessage::Result {
            request_id: request.request_id.clone(),
            attempt_id: request.attempt_id.clone(),
            outcome: CellOutcome::Cancelled {
                request_id: request.request_id,
                attempt_id: request.attempt_id,
            },
        },
    );
    let executed = settled(submitting).await;
    assert!(
        matches!(executed.outcome, CellOutcome::Cancelled { .. }),
        "the machine's own answer ends the wait: {:?}",
        executed.outcome
    );
    assert!(executed.grant_without_result.is_none());
}

/// What a machine's posted state says about one attempt, strongest first,
/// and a link judged silent says nothing whatever it last posted.
#[test]
fn the_account_of_a_claimed_attempt_reads_the_machines_own_lists() {
    let request = provenance_request();
    let key = (request.request_id.clone(), request.attempt_id.clone());
    let read = |snapshot: &FleetSnapshot, link_silent| {
        account_of_claimed_attempt(snapshot, link_silent, &key)
    };
    assert_eq!(
        read(&granted_snapshot(&request, false), false),
        ClaimedAttemptAccount::Held
    );
    assert_eq!(
        read(&granted_snapshot(&request, true), false),
        ClaimedAttemptAccount::Executing
    );
    assert_eq!(
        read(&FleetSnapshot::default(), false),
        ClaimedAttemptAccount::NotListed
    );
    let other = FleetSnapshot {
        executing_requests: vec![executing_fixture("someone-else")],
        ..FleetSnapshot::default()
    };
    assert_eq!(read(&other, false), ClaimedAttemptAccount::NotListed);
    assert_eq!(
        read(&granted_snapshot(&request, true), true),
        ClaimedAttemptAccount::LinkSilent,
        "a listing on a link that stopped reporting is not progress"
    );
}

/// Progress resets the quiet clock whenever it is read; only an absence
/// that lasts the whole bound ends the wait, and it is measured from the
/// grant when the machine never listed the attempt at all.
#[test]
fn only_an_absence_lasting_the_bound_ends_a_claimed_wait() {
    const BOUND: u64 = 600_000;
    let granted = 1_000_000;
    let mut watch = ClaimedAttemptWatch::granted_at(granted);
    assert_eq!(
        watch.observe(ClaimedAttemptAccount::NotListed, granted + BOUND - 1, BOUND),
        None
    );
    // Hours of execution, read every few seconds, never end it.
    for hour in 1..=6 {
        let at = granted + hour * 3_600_000;
        assert_eq!(
            watch.observe(ClaimedAttemptAccount::Executing, at, BOUND),
            None
        );
    }
    let last = granted + 6 * 3_600_000;
    assert_eq!(
        watch.observe(ClaimedAttemptAccount::LinkDown, last + BOUND - 1, BOUND),
        None,
        "a link gap shorter than the bound is waited out"
    );
    assert_eq!(
        watch.observe(ClaimedAttemptAccount::LinkDown, last + BOUND, BOUND),
        Some(ClaimedAttemptSilence {
            quiet_ms: BOUND,
            last_progress: Some(ClaimedAttemptAccount::Executing),
            final_account: ClaimedAttemptAccount::LinkDown,
        })
    );

    let mut never = ClaimedAttemptWatch::granted_at(granted);
    assert_eq!(
        never.observe(ClaimedAttemptAccount::NotListed, granted + BOUND, BOUND),
        Some(ClaimedAttemptSilence {
            quiet_ms: BOUND,
            last_progress: None,
            final_account: ClaimedAttemptAccount::NotListed,
        })
    );
}

/// The row says why in the attempt's own terms, and never reaches for a
/// substrate label or a capacity reading taken at the instant of giving
/// up, which is what made a live link read as `ConnectedStalled`.
#[test]
fn a_claimed_attempts_silence_is_described_in_its_own_terms() {
    let key = ("request".to_string(), "attempt".to_string());
    let outcome = ClaimedAttemptSilence {
        quiet_ms: 600_000,
        last_progress: Some(ClaimedAttemptAccount::Executing),
        final_account: ClaimedAttemptAccount::LinkDown,
    }
    .outcome(&key, "bgsrv-ub".into());
    let CellOutcome::Unavailable { reason, diagnostic } = outcome else {
        panic!("silence is an unavailable outcome");
    };
    assert!(matches!(
        reason,
        CellUnavailableReason::ClaimedAttemptSilent {
            quiet_ms: 600_000,
            ..
        }
    ));
    for fact in [
        "bgsrv-ub",
        "request attempt",
        "10m00s",
        "was not connected",
        "listed it as running a command",
        "rather than on elapsed time",
    ] {
        assert!(diagnostic.contains(fact), "missing {fact}: {diagnostic}");
    }
    for absent in ["ConnectedStalled", "capacity", "budget", "watchdog"] {
        assert!(!diagnostic.contains(absent), "{absent} in {diagnostic}");
    }
}

/// A link that drops under a held claim and never comes back is one drop,
/// counted from the claim's own history. Nothing here reattaches, so a
/// count that implied a reconnection would be claiming something that did
/// not happen.
#[tokio::test]
async fn a_link_that_drops_and_stays_down_is_counted_as_one_drop() {
    let temp = tempfile::tempdir().unwrap();
    let orch = Arc::new(test_orchestrator(temp.path()).await);
    let (generation, submitting, _machine) = granted_and_held(&orch).await;
    assert!(orch
        .fleet
        .disconnect_advertised_executor(COLOCATED_EXECUTOR_ID, generation));
    let executed = settled(submitting).await;

    assert_eq!(
        silent_accounts(&executed.outcome).map(|(_, now)| now),
        Some(ClaimedAttemptAccount::LinkDown),
        "{:?}",
        executed.outcome
    );
    let grant = executed
        .grant_without_result
        .unwrap_or_else(|| panic!("no grant account: {:?}", executed.outcome));
    assert_eq!(grant.connection_drops, Some(1), "{:?}", executed.outcome);
}

/// An executor's own refusal after a grant is not proof nothing ran. A
/// sequential batch whose first item finished comes back as `Spawn` when
/// the cache checkpoint after it fails, so that report carries the grant
/// like any other outcome without a result.
#[tokio::test]
async fn an_executor_refusal_after_a_grant_still_carries_the_grant() {
    let temp = tempfile::tempdir().unwrap();
    let orch = Arc::new(test_orchestrator(temp.path()).await);
    let (generation, submitting, _machine) = granted_and_held(&orch).await;
    let request = provenance_request();
    orch.fleet.handle_executor_message(
        COLOCATED_EXECUTOR_ID,
        generation,
        ExecutorMessage::Result {
            request_id: request.request_id,
            attempt_id: request.attempt_id,
            outcome: CellOutcome::Unavailable {
                reason: CellUnavailableReason::Spawn,
                diagnostic: "cache checkpoint callback: runner refused".into(),
            },
        },
    );
    let executed = settled(submitting).await;

    assert!(
        matches!(
            executed.outcome,
            CellOutcome::Unavailable {
                reason: CellUnavailableReason::Spawn,
                ..
            }
        ),
        "{:?}",
        executed.outcome
    );
    assert!(
        executed.grant_without_result.is_some(),
        "a refusal after a grant does not prove nothing ran"
    );
}

/// Everything the claim took is back: its identity binds again, and the
/// next offer for the same project claims on the same link. A leaked ticket
/// fails the second half as provisioning for the withdrawn request. Both
/// halves run on the blocking pool, as the machine's link handler would
/// run them.
async fn assert_machine_released(
    machine: &cairn_exec::Fleet,
    summary: &cairn_common::executor_protocol::OfferSummary,
) {
    let (machine, summary) = (machine.clone(), summary.clone());
    cairn_exec::blocking::offload(move || {
        assert!(
            machine.bind_request_identity(&summary.request_id),
            "the withdrawn claim's request identity is released"
        );
        machine.release_request_identity(&summary.request_id);
        let mut next = summary.clone();
        next.request_id = format!("{}-next", summary.request_id);
        machine
            .try_claim("next-entry", &next)
            .expect("the next offer for the same project claims without reconnecting");
    })
    .await;
}

/// A pending route records that an acquisition may have reached an executor
/// the runner never heard back from, and while that executor answers, the
/// record is worth honoring: the cell might be there, and placing a second
/// one would split the owner across two environments.
///
/// Once the connection is gone the record names a session that ended. Holding
/// the owner to it is what left jobs unable to run anywhere at all, because
/// nothing else ever removes such a route.
#[test]
fn a_pending_route_binds_its_owner_only_while_that_executor_answers() {
    let pool = Fleet::default();
    pool.lock_connections().extend([
        answering_fleet_entry("first", "linux", 10, &[]),
        answering_fleet_entry("second", "linux", 0, &[]),
    ]);
    let mut request = dev_instance_request("lease");
    pool.reserve_pending_residency_route(ResidencyRoute {
        holder: request.holder.clone(),
        repository: request.repository.clone(),
        executor_id: "first".into(),
        pending: true,
    })
    .unwrap();
    assert_eq!(
        pool.resolve_residency_acquire_route(&mut request)
            .unwrap()
            .unwrap()
            .executor_id,
        "first"
    );

    // Dispatch was accepted, but neither a response nor an occupant snapshot
    // arrived before the owning connection disappeared for good.
    pool.lock_connections().remove("first");
    machine_left(&pool, "first");
    assert!(pool
        .resolve_residency_acquire_route(&mut request)
        .unwrap()
        .is_none());
    assert!(pool.residency_routes.lock().unwrap().is_empty());
}

/// Put an executor's link outage far enough in the past that the runner has
/// stopped reading it as a flap.
/// A connection that answered just now. Residency routing judges a route by
/// the staleness rule dispatch uses, so a home that a test means to be live
/// has to have spoken recently, not at the fixed test instant.
fn answering_fleet_entry(
    id: &str,
    os: &str,
    load: usize,
    warm: &[&str],
) -> (String, ExecutorConnectionState) {
    fleet_entry_at(id, os, load, warm, unix_time_ms())
}

fn machine_left(pool: &Fleet, executor_id: &str) {
    pool.stage_link_absent(executor_id, ENVIRONMENT_LEASE_GRACE_MS + 1);
}

/// A machine is lost when it has gone unanswered for the grace in time the
/// runner was awake. The hour here is the runner's own host asleep: the link
/// closed under it, and the machine had answered ten awake seconds earlier,
/// so neither the owner's wake nor eviction may fire over a healthy box.
#[test]
fn a_runners_own_sleep_is_not_charged_to_a_machine_whose_link_closed() {
    let (pool, mut request, inbox) = pool_with_a_silent_home(3_600_000);
    drop(inbox);
    pool.lock_connections()
        .get("bbs")
        .unwrap()
        .link_clock
        .stage(10_000, 10_000);

    assert!(!pool.machine_lost_past_grace("bbs"));
    assert!(pool.resolve_residency_acquire_route(&mut request).is_err());
    assert_eq!(pool.residency_routes.lock().unwrap().len(), 1);
    assert!(pool.take_environment_eviction(&request.holder).is_none());

    // The same closed link after the grace of awake time really has passed.
    pool.lock_connections()
        .get("bbs")
        .unwrap()
        .link_clock
        .stage(ENVIRONMENT_LEASE_GRACE_MS + 1_000, 10_000);
    assert!(pool.machine_lost_past_grace("bbs"));
}

/// Absence is counted the same way, and reconnecting ends it.
#[test]
fn an_absent_machine_is_lost_only_past_the_grace_and_until_it_returns() {
    let pool = Fleet::default();
    pool.stage_link_absent("pi1", 10_000);
    assert!(!pool.machine_lost_past_grace("pi1"));
    machine_left(&pool, "pi1");
    assert!(pool.machine_lost_past_grace("pi1"));
    pool.lock_connections()
        .extend([answering_fleet_entry("pi1", "linux", 0, &[])]);
    assert!(!pool.machine_lost_past_grace("pi1"));
}

/// An unnamed service waits for the runner's own machine at boot, so a
/// remote that attaches first does not take it. The wait is bounded, for a
/// runner whose executor never comes, and belongs to the first attach only.
#[tokio::test]
async fn an_unnamed_placement_waits_only_for_the_first_colocated_attach() {
    let pool = Fleet::default();
    assert!(
        !pool
            .wait_for_first_colocated_attach(std::time::Duration::from_millis(20))
            .await
    );

    let waiting = {
        let pool = pool.clone();
        tokio::spawn(async move {
            pool.wait_for_first_colocated_attach(std::time::Duration::from_secs(60))
                .await
        })
    };
    tokio::task::yield_now().await;
    let (sender, _inbox) = mpsc::unbounded_channel();
    let link = pool.attach_executor_with_toolchains(sender, &["bun"]);
    assert!(waiting.await.unwrap());

    assert!(pool.disconnect_advertised_executor(COLOCATED_EXECUTOR_ID, link));
    assert!(
        pool.wait_for_first_colocated_attach(std::time::Duration::ZERO)
            .await,
        "a later absence is placement's to route around"
    );
}

/// A runner restart keeps a service on its machine while that machine
/// answers: the route is persisted, and an acquisition converges on it
/// rather than ranking the fleet again, even where the ranking would choose
/// somewhere else.
#[test]
fn a_restarted_runner_acquires_a_service_where_its_persisted_route_says() {
    let temp = tempfile::tempdir().unwrap();
    let route_path = temp.path().join("routes.json");
    let request = fleet_residency_request(ResidencyHolder::Service {
        service_id: "thread:p:t-1:bugs".into(),
    });
    Fleet::with_residency_route_path(route_path.clone())
        .update_residency_routes(|routes| {
            routes.insert(
                ("pi1".into(), request.holder.storage_key()),
                ResidencyRoute {
                    holder: request.holder.clone(),
                    repository: request.repository.clone(),
                    executor_id: "pi1".into(),
                    pending: false,
                },
            );
        })
        .unwrap();

    let restarted = Fleet::with_residency_route_path(route_path);
    restarted.lock_connections().extend([
        answering_fleet_entry("pi1", "linux", 3, &[]),
        answering_fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]),
    ]);
    let mut request = request;
    let selected = restarted
        .resolve_residency_acquire_route(&mut request)
        .unwrap()
        .expect("a live route settles the placement");
    assert_eq!(selected.executor_id, "pi1");
}

/// cairn/4931 specimen A: bbs holds a job's environment with terminal `dev`
/// running in it, and its link stays in the connection table while the
/// machine says nothing. `quiet_ms` is how long ago it last answered.
fn pool_with_a_silent_home(
    quiet_ms: u64,
) -> (
    Fleet,
    ResidencyAcquireRequest,
    mpsc::UnboundedReceiver<ExecutorMessage>,
) {
    let pool = Fleet::default();
    pool.declare_enrolled_remote("bbs", "bbs", "linux", "x86_64");
    let (sender, inbox) = mpsc::unbounded_channel();
    let (id, mut entry) = fleet_entry("bbs", "linux", 0, &[]);
    entry.sender = sender;
    entry.last_progress_unix_ms = unix_time_ms().saturating_sub(quiet_ms);
    let mut cell = materialization_read_cell("bbs-home", "silent");
    cell.occupancy.processes.insert(
        "terminal:dev".into(),
        cairn_common::executor_protocol::ResidentProcess {
            generation: 1,
            kind: cairn_common::executor_protocol::ResidentProcessKind::Terminal {
                slug: "dev".into(),
            },
            spec: None,
            status: cairn_common::executor_protocol::ResidentProcessStatus::Starting,
            reservation: None,
            live_usage: None,
            charge: None,
        },
    );
    Arc::make_mut(&mut entry.snapshot).cells = vec![cell];
    pool.lock_connections().insert(id, entry);
    pool.lock_connections()
        .extend([answering_fleet_entry("second", "linux", 0, &[])]);
    let request = fleet_residency_request(ResidencyHolder::Job {
        job_id: "silent".into(),
    });
    pool.residency_routes.lock().unwrap().insert(
        ("bbs".into(), request.holder.storage_key()),
        ResidencyRoute {
            holder: request.holder.clone(),
            repository: request.repository.clone(),
            executor_id: "bbs".into(),
            pending: false,
        },
    );
    (pool, request, inbox)
}

/// Presence in the connection table is not liveness. A machine that stays
/// attached and says nothing for the whole lease grace is gone for the
/// purposes of its environment, exactly like one that disconnected: the
/// environment is rebuilt, the cause and the process it cost are stated,
/// the executors page records it, and the silent machine is told to let go
/// of the cell in case it ever answers again.
#[test]
fn a_silent_home_still_in_the_connection_table_is_evicted_after_the_lease_grace() {
    let (pool, mut request, mut inbox) =
        pool_with_a_silent_home(ENVIRONMENT_LEASE_GRACE_MS + 1_000);

    assert!(pool
        .resolve_residency_acquire_route(&mut request)
        .unwrap()
        .is_none());
    assert!(pool.residency_routes.lock().unwrap().is_empty());
    let eviction = pool
        .take_environment_eviction(&request.holder)
        .expect("the loss is reported");
    assert_eq!(eviction.executor, "bbs");
    assert!(matches!(
        eviction.cause,
        EvictionCause::Silent { generation: 1, silent_ms } if silent_ms > ENVIRONMENT_LEASE_GRACE_MS
    ));
    assert_eq!(eviction.lost_processes, vec!["terminal `dev`".to_string()]);

    let timeline = pool.enrolled_remotes.lock().unwrap()["bbs"]
        .connection_timeline
        .clone();
    let evicted = timeline
        .iter()
        .find(|transition| transition.phase == RemoteConnectionPhase::EnvironmentEvicted)
        .expect("the executors page records the eviction");
    assert_eq!(evicted.generation, Some(1));
    let reason = evicted.reason.as_deref().unwrap();
    assert!(
        reason.starts_with("job silent's execution environment rebuilt elsewhere: silent 5m"),
        "{reason}"
    );
    assert!(reason.ends_with("on generation 1"), "{reason}");

    assert!(matches!(
        inbox.try_recv(),
        Ok(ExecutorMessage::ResidencyRequest {
            operation: ResidencyOperation::Release { .. },
            ..
        })
    ));
}

/// Silence that has not yet outlasted the grace holds the environment, and
/// the caller waits rather than being refused. The diagnostic names the
/// silence, when the rebuild will happen, and what it would cost.
#[test]
fn a_silent_home_within_the_grace_is_held_and_says_when_it_will_be_rebuilt() {
    let (pool, mut request, _inbox) =
        pool_with_a_silent_home(EXECUTOR_PROGRESS_FRESHNESS_MS + 17_000);

    let refusal = pool
        .resolve_residency_acquire_route(&mut request)
        .unwrap_err();
    let ResidencyResult::Failed {
        kind,
        diagnostic,
        cell_outcome,
    } = &refusal
    else {
        panic!("a home inside its grace refuses with a classified failure");
    };
    assert_eq!(*kind, ResidencyFailureKind::Admission);
    assert!(
        diagnostic.contains("(bbs) is connected but has not answered for 1m32s"),
        "{diagnostic}"
    );
    assert!(
        diagnostic.contains("rebuilt elsewhere in 3m28s"),
        "{diagnostic}"
    );
    assert!(
        diagnostic.contains("terminal `dev` running there would not survive"),
        "{diagnostic}"
    );
    assert!(placement::classify_residency_failure(
        kind,
        cell_outcome.as_deref(),
        pool.residency_link_restoration(&request.holder)
    )
    .is_capacity());
    assert_eq!(pool.residency_routes.lock().unwrap().len(), 1);
    assert!(pool.take_environment_eviction(&request.holder).is_none());
}

/// Two routes for one owner, one machine answering and one silent, is not a
/// contradiction: the silent one cannot serve a batch, so the answering one
/// holds the environment and the silent route is forgotten.
#[test]
fn a_silent_route_beside_a_live_one_yields_to_the_live_one() {
    let (pool, mut request, _inbox) =
        pool_with_a_silent_home(EXECUTOR_PROGRESS_FRESHNESS_MS + 1_000);
    pool.residency_routes.lock().unwrap().insert(
        ("second".into(), request.holder.storage_key()),
        ResidencyRoute {
            holder: request.holder.clone(),
            repository: request.repository.clone(),
            executor_id: "second".into(),
            pending: false,
        },
    );

    let selected = pool
        .resolve_residency_acquire_route(&mut request)
        .expect("no conflicting declaration")
        .expect("the answering machine holds the environment");
    assert_eq!(selected.executor_id, "second");
    let routes = pool.residency_routes.lock().unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(routes.values().next().unwrap().executor_id, "second");
}

/// The mistake the grace exists to prevent, and the reason absence alone
/// cannot decide this: a transport that dropped leaves the executor process
/// running and its cells intact, so rebuilding on the first missed
/// connection would spend an agent's uncommitted files on a network blip.
/// The environment is held and the caller waits; only a machine still
/// unreachable when the grace runs out is treated as gone.
#[test]
fn a_link_that_dropped_is_not_yet_a_machine_that_left() {
    let pool = Fleet::default();
    pool.lock_connections().extend([
        fleet_entry("first", "linux", 0, &[]),
        fleet_entry("second", "linux", 0, &[]),
    ]);
    let mut request = fleet_residency_request(ResidencyHolder::Job {
        job_id: "flapping".into(),
    });
    pool.residency_routes.lock().unwrap().insert(
        ("first".into(), request.holder.storage_key()),
        ResidencyRoute {
            holder: request.holder.clone(),
            repository: request.repository.clone(),
            executor_id: "first".into(),
            pending: false,
        },
    );

    // The link goes down the way an ordinary peer or I/O close takes it.
    assert!(pool.disconnect_advertised_executor("first", 1));
    let refusal = pool
        .resolve_residency_acquire_route(&mut request)
        .unwrap_err();
    let ResidencyResult::Failed {
        kind, cell_outcome, ..
    } = &refusal
    else {
        panic!("a machine that may still answer must refuse with a classified failure");
    };
    assert!(placement::classify_residency_failure(
        kind,
        cell_outcome.as_deref(),
        pool.residency_link_restoration(&request.holder)
    )
    .is_capacity());
    assert_eq!(pool.residency_routes.lock().unwrap().len(), 1);
    assert!(pool.take_environment_eviction(&request.holder).is_none());

    // The same outage, once it has outlasted the grace.
    machine_left(&pool, "first");
    assert!(pool
        .resolve_residency_acquire_route(&mut request)
        .unwrap()
        .is_none());
    assert!(pool.residency_routes.lock().unwrap().is_empty());
    assert!(pool.take_environment_eviction(&request.holder).is_some());
}

/// A runner restart carries the route authority forward into a process where
/// nothing is connected yet, which is exactly when a route is a claim about
/// the past rather than a fact about the present. It binds an owner only
/// while the machine it names is there to hold the environment, and the
/// eviction is persisted so the next restart does not resurrect it.
#[test]
fn runner_restart_does_not_bind_an_owner_to_a_machine_that_never_returns() {
    let temp = tempfile::tempdir().unwrap();
    let route_path = temp.path().join("lifetime-routes.json");
    let first = Fleet::with_residency_route_path(route_path.clone());
    let request = dev_instance_request("lease-restart");
    first
        .reserve_pending_residency_route(ResidencyRoute {
            holder: request.holder.clone(),
            repository: request.repository.clone(),
            executor_id: "first".into(),
            pending: true,
        })
        .unwrap();
    drop(first);

    let replacement = Fleet::with_residency_route_path(route_path.clone());
    replacement
        .lock_connections()
        .insert("second".into(), fleet_entry("second", "linux", 0, &[]).1);
    let mut retry = request;
    machine_left(&replacement, "first");
    assert!(replacement
        .resolve_residency_acquire_route(&mut retry)
        .unwrap()
        .is_none());
    assert!(replacement.residency_routes.lock().unwrap().is_empty());
    assert!(Fleet::with_residency_route_path(route_path)
        .residency_routes
        .lock()
        .unwrap()
        .is_empty());
}

/// The whole of CAIRN-4332 in its smallest shape: an owner whose environment
/// was on a machine that is gone is placed again on a machine that is here,
/// rather than refused for as long as the route survives — which was
/// forever, since only that machine's own snapshots could have retired it.
#[test]
fn an_environment_whose_executor_is_gone_is_rebuilt_rather_than_refused() {
    let pool = Fleet::default();
    pool.lock_connections()
        .insert("second".into(), fleet_entry("second", "linux", 0, &[]).1);
    let mut request = fleet_residency_request(ResidencyHolder::Job {
        job_id: "stranded".into(),
    });
    pool.residency_routes.lock().unwrap().insert(
        ("first".into(), request.holder.storage_key()),
        ResidencyRoute {
            holder: request.holder.clone(),
            repository: request.repository.clone(),
            executor_id: "first".into(),
            pending: false,
        },
    );
    machine_left(&pool, "first");

    assert!(pool
        .resolve_residency_acquire_route(&mut request)
        .unwrap()
        .is_none());
    assert!(pool.residency_routes.lock().unwrap().is_empty());
    // The loss is a fact the agent is owed, exactly once: its next batch runs
    // in a fresh checkout, and anything only the old environment held is gone.
    assert!(matches!(
        pool.take_environment_eviction(&request.holder),
        Some(EnvironmentEviction {
            executor,
            cause: EvictionCause::Disconnected { .. },
            ..
        }) if executor == "first"
    ));
    assert!(pool.take_environment_eviction(&request.holder).is_none());
}

/// A supervised link that is bouncing is not a machine that is gone, and the
/// difference decides opposite answers: the environment is seconds away, so
/// the batch waits for it instead of abandoning a cell that is about to
/// answer again. Only the runner's own restoration evidence buys that wait,
/// and the moment it stops, the environment is rebuilt like any other.
#[test]
fn an_environment_waits_out_a_colocated_link_the_runner_is_restoring() {
    let pool = Fleet::default();
    let mut request = fleet_residency_request(ResidencyHolder::Job {
        job_id: "bouncing".into(),
    });
    pool.residency_routes.lock().unwrap().insert(
        (COLOCATED_EXECUTOR_ID.into(), request.holder.storage_key()),
        ResidencyRoute {
            holder: request.holder.clone(),
            repository: request.repository.clone(),
            executor_id: COLOCATED_EXECUTOR_ID.into(),
            pending: false,
        },
    );
    pool.declare_colocated_substrate(ExecutorSubstrateState::SupervisorRespawning);

    let refusal = pool
        .resolve_residency_acquire_route(&mut request)
        .unwrap_err();
    let ResidencyResult::Failed {
        kind, cell_outcome, ..
    } = &refusal
    else {
        panic!("a restoring link must refuse with a classified residency failure");
    };
    assert!(placement::classify_residency_failure(
        kind,
        cell_outcome.as_deref(),
        pool.residency_link_restoration(&request.holder)
    )
    .is_capacity());
    assert_eq!(pool.residency_routes.lock().unwrap().len(), 1);
    assert!(pool.take_environment_eviction(&request.holder).is_none());

    pool.clear_colocated_substrate();
    machine_left(&pool, COLOCATED_EXECUTOR_ID);
    assert!(pool
        .resolve_residency_acquire_route(&mut request)
        .unwrap()
        .is_none());
    assert!(pool.residency_routes.lock().unwrap().is_empty());
}

/// The other half of re-placement: a machine that comes back still holding an
/// owner's old cell must not become a second home for it. Two routes for one
/// owner is an ambiguity no acquisition can resolve, so the leftover is
/// released and the live environment keeps the owner.
#[test]
fn a_returning_executor_releases_an_environment_that_moved_while_it_was_away() {
    let pool = Fleet::default();
    pool.lock_connections()
        .insert("second".into(), fleet_entry("second", "linux", 0, &[]).1);
    let holder = ResidencyHolder::Job {
        job_id: "moved".into(),
    };
    pool.residency_routes.lock().unwrap().insert(
        ("second".into(), holder.storage_key()),
        ResidencyRoute {
            holder: holder.clone(),
            repository: fleet_residency_request(holder.clone()).repository,
            executor_id: "second".into(),
            pending: false,
        },
    );

    let (returning, mut inbox) = mpsc::unbounded_channel();
    let (id, mut entry) = fleet_entry("first", "linux", 0, &[]);
    entry.sender = returning;
    let generation = entry.generation;
    pool.lock_connections().insert(id, entry);
    assert!(pool.set_executor_snapshot(
        "first",
        generation,
        FleetSnapshot {
            cells: vec![materialization_read_cell("leftover-cell", "moved")],
            ..FleetSnapshot::default()
        },
        ExecutorSubstrateReport::default(),
    ));

    let routes = pool.residency_routes.lock().unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(routes.values().next().unwrap().executor_id, "second");
    drop(routes);
    assert!(matches!(
        inbox.try_recv(),
        Ok(ExecutorMessage::ResidencyRequest {
            operation: ResidencyOperation::Release { .. },
            ..
        })
    ));
}

/// A job home is where the job's work already is, and a machine that is gone
/// is not that. Reading a dead route as occupancy is what measured a batch's
/// selector against a machine nothing could run on.
#[test]
fn a_job_home_on_a_departed_machine_is_not_read_as_occupancy() {
    let pool = Fleet::default();
    pool.lock_connections()
        .extend([fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[])]);
    let holder = ResidencyHolder::Job {
        job_id: "departed".into(),
    };
    pool.residency_routes.lock().unwrap().insert(
        ("first".into(), holder.storage_key()),
        ResidencyRoute {
            holder: holder.clone(),
            repository: fleet_residency_request(holder.clone()).repository,
            executor_id: "first".into(),
            pending: false,
        },
    );
    assert_eq!(
        pool.job_home_executor(&holder).as_deref(),
        Some(COLOCATED_EXECUTOR_ID)
    );
}

/// A job whose environment is on remote `bglab-mac`, whose link has dropped,
/// with `local` connected beside it.
fn pool_with_home_on_an_absent_remote() -> (Fleet, ResidencyHolder) {
    let pool = Fleet::default();
    let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "macos", 0, &[]);
    local.colocated = true;
    pool.lock_connections().insert(local_id, local);
    pool.declare_enrolled_remote("bglab-mac", "bglab-mac", "macos", "aarch64");
    let holder = ResidencyHolder::Job {
        job_id: "reconnecting".into(),
    };
    pool.residency_routes.lock().unwrap().insert(
        ("bglab-mac".into(), holder.storage_key()),
        ResidencyRoute {
            holder: holder.clone(),
            repository: fleet_residency_request(holder.clone()).repository,
            executor_id: "bglab-mac".into(),
            pending: false,
        },
    );
    (pool, holder)
}

/// cairn/4712 review: while the home's link is down, the prediction is the
/// recorded machine, never the colocated guess. Guessing `local` made a
/// selector for `local` look like it matched the home, so a verdict batch
/// waited on the absent remote's acquisition, and a committing batch naming
/// the remote was refused as if its terminal were on `local`.
#[test]
fn an_absent_home_is_predicted_where_its_route_says_not_on_the_colocated_machine() {
    let (pool, holder) = pool_with_home_on_an_absent_remote();
    assert_eq!(
        pool.predicted_job_home(&holder),
        Some(PredictedJobHome {
            executor_id: "bglab-mac".into(),
            recorded: true,
        })
    );
    let named = |name: &str| ExecutorSelector {
        name: Some(name.into()),
        ..ExecutorSelector::default()
    };
    assert_eq!(
        pool.recorded_executor_satisfies_selector("bglab-mac", &named("local")),
        Some(false),
        "a verdict batch for local detaches instead of waiting on bglab-mac"
    );
    assert_eq!(
        pool.recorded_executor_satisfies_selector("bglab-mac", &named("bglab-mac")),
        Some(true),
        "a batch naming the terminal's own machine stays home"
    );
    let toolchains = ExecutorSelector {
        required_toolchains: vec!["rust".into()],
        ..ExecutorSelector::default()
    };
    assert_eq!(
        pool.recorded_executor_satisfies_selector("bglab-mac", &toolchains),
        None,
        "what only a live advertisement can say is unknown, not a mismatch"
    );
}

#[test]
fn with_no_route_the_prediction_is_an_unrecorded_colocated_guess() {
    let pool = Fleet::default();
    let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "macos", 0, &[]);
    local.colocated = true;
    pool.lock_connections().insert(local_id, local);
    let holder = ResidencyHolder::Job {
        job_id: "fresh".into(),
    };
    assert_eq!(
        pool.predicted_job_home(&holder),
        Some(PredictedJobHome {
            executor_id: COLOCATED_EXECUTOR_ID.into(),
            recorded: false,
        })
    );
}

#[test]
fn duplicate_fleet_lifetime_snapshots_fail_closed() {
    let pool = Fleet::default();
    pool.lock_connections().extend([
        answering_fleet_entry("first", "linux", 0, &[]),
        answering_fleet_entry("second", "linux", 0, &[]),
    ]);
    let mut request = dev_instance_request("lease");
    let mut routes = pool.residency_routes.lock().unwrap();
    for executor_id in ["first", "second"] {
        routes.insert(
            (executor_id.into(), "lease".into()),
            ResidencyRoute {
                holder: request.holder.clone(),
                repository: request.repository.clone(),
                executor_id: executor_id.into(),
                pending: false,
            },
        );
    }
    drop(routes);
    assert!(matches!(
        pool.resolve_residency_acquire_route(&mut request),
        Err(ResidencyResult::Failed {
            kind: ResidencyFailureKind::ConflictingDeclaration,
            ..
        })
    ));
}

pub(super) fn targeted_request(os: &str) -> CellRequest {
    CellRequest {
        request_id: "r".into(),
        attempt_id: "a".into(),
        project_id: "p".into(),
        repository: RepositoryLocator::ColocatedPath {
            project_id: "p".into(),
            repository_id: "repo".into(),
            absolute_path: "/repo".into(),
        },
        base_commit: "base".into(),
        command: "true".into(),
        command_class: cairn_common::executor_protocol::CellCommandClass::Other,
        placement_work_class: cairn_common::executor_protocol::PlacementWorkClass::AgentSessions,
        owner: None,
        cwd: String::new(),
        env: Vec::new(),
        priority: CellPriority::ReviewCheck,
        wait_horizon_unix_ms: unix_time_ms() + 1_000,
        waiting_since_unix_ms: 0,
        timeout_ms: 1_000,
        mutation_policy: MutationPolicy::PureVerdict,
        requesting_job_id: None,
        affinity_key: None,
        executor: Some(ExecutorSelector {
            os: Some(os.into()),
            ..ExecutorSelector::default()
        }),
        pinned_executor_id: None,
        placement_mobility: Default::default(),
        verdict_platforms: Vec::new(),
        avoided_executor_ids: Vec::new(),
        project_requirements: Default::default(),
        command_resource_identity: None,
        resource_reservation: Default::default(),
        learned_estimate: None,
        scratch_sync: None,
        home_affinity: None,
        publication: None,
    }
}

#[test]
fn a_windows_targeted_refusal_names_the_absent_windows_machine_and_its_cause() {
    let request = targeted_request("windows");
    let absent = EnrolledRemote {
        reach_history: None,
        executor_id: "bglab-win".into(),
        name: "bglab-win".into(),
        os: "windows".into(),
        arch: "x86_64".into(),
        link: RemoteLinkState::BootstrapFailed {
            cause: "ssh connection timed out".into(),
        },
        last_attempt: None,
        drained: false,
        drain_state: Default::default(),
        role: Default::default(),
        last_seen_unix_ms: None,
        retired: Vec::new(),
        connection_timeline: Vec::new(),
        stranded: None,
        unreturned_update: None,
        absence_since_unix_ms: None,
        update_returns_by_unix_ms: None,
    };
    let diagnostic = no_matching_executor_diagnostic(&HashMap::new(), &[absent], &request, &[]);
    assert!(diagnostic.contains("bglab-win (windows/x86_64)"));
    assert!(diagnostic.contains("ssh connection timed out"));
}

#[test]
fn a_windows_targeted_refusal_says_when_no_windows_machine_is_enrolled() {
    let request = targeted_request("windows");
    let linux = EnrolledRemote {
        reach_history: None,
        executor_id: "bglab-ub".into(),
        name: "bglab-ub".into(),
        os: "linux".into(),
        arch: "x86_64".into(),
        link: RemoteLinkState::Absent {
            last_beat_unix_ms: None,
            last_reason: "offline".into(),
        },
        last_attempt: None,
        drained: false,
        drain_state: Default::default(),
        role: Default::default(),
        last_seen_unix_ms: None,
        retired: Vec::new(),
        connection_timeline: Vec::new(),
        stranded: None,
        unreturned_update: None,
        absence_since_unix_ms: None,
        update_returns_by_unix_ms: None,
    };
    let diagnostic = no_matching_executor_diagnostic(&HashMap::new(), &[linux], &request, &[]);
    assert!(diagnostic.contains("No enrolled machine on windows exists."));
    assert!(!diagnostic.contains("bglab-ub"));
}

/// A pin is a question about one machine. Answering it with the whole
/// absent fleet sends an operator to fix a host the work would have refused
/// anyway, which is the same defect `PinMismatch` exists to prevent inside
/// `candidate_rejection`.
#[test]
fn a_pinned_refusal_names_only_the_machine_the_work_is_pinned_to() {
    let mut request = targeted_request("linux");
    request.executor = None;
    request.pinned_executor_id = Some("execution-home".into());
    let pinned = EnrolledRemote {
        reach_history: None,
        executor_id: "execution-home".into(),
        name: "bglab-ub".into(),
        os: "linux".into(),
        arch: "x86_64".into(),
        link: RemoteLinkState::Absent {
            last_beat_unix_ms: None,
            last_reason: "offline".into(),
        },
        last_attempt: None,
        drained: false,
        drain_state: Default::default(),
        role: Default::default(),
        last_seen_unix_ms: None,
        retired: Vec::new(),
        connection_timeline: Vec::new(),
        stranded: None,
        unreturned_update: None,
        absence_since_unix_ms: None,
        update_returns_by_unix_ms: None,
    };
    let bystander = EnrolledRemote {
        executor_id: "elsewhere".into(),
        name: "bglab-sp1".into(),
        ..pinned.clone()
    };

    let diagnostic =
        no_matching_executor_diagnostic(&HashMap::new(), &[pinned, bystander], &request, &[]);

    assert!(diagnostic.contains("bglab-ub (linux/x86_64)"));
    assert!(!diagnostic.contains("bglab-sp1"));
}

#[test]
fn a_platform_selector_routes_only_to_matching_executor() {
    let connections = HashMap::from([
        fleet_entry("linux", "linux", 0, &[]),
        fleet_entry("windows", "windows", 0, &[]),
    ]);
    let selected = choose_executor(&connections, &targeted_request("windows"))
        .unwrap()
        .unwrap();
    assert_eq!(selected.executor_id, "windows");
}

/// The managed-object repository every `fleet_entry` serves, under
/// `repository_id`, holding `commits` as warm roots.
fn test_warm_roots(repository_id: &str, commits: &[&str]) -> WarmRepositoryRoots {
    WarmRepositoryRoots {
        repository: RepositoryLocator::ManagedObjects {
            project_id: "p".into(),
            repository_id: repository_id.into(),
            object_format: GitObjectFormat::Sha1,
        }
        .identity(),
        commits: commits.iter().map(|commit| commit.to_string()).collect(),
    }
}

/// A delta against a revision the runner does not hold changes nothing and
/// asks for the whole inventory once; the whole answer replaces the set;
/// a change from a retired link is refused.
#[test]
fn an_unmatched_warm_root_delta_asks_once_for_the_whole_inventory() {
    let fleet = Fleet::default();
    let (sender, mut frames) = mpsc::unbounded_channel();
    let (_, entry) = fleet_entry("gap", "linux", 0, &[]);
    let generation = fleet
        .attach_advertised_executor(
            (*entry.advertisement).clone(),
            WarmRootInventory {
                revision: 2,
                repositories: vec![test_warm_roots("repo", &["a1"])],
            },
            sender,
            false,
            None,
            None,
        )
        .unwrap();
    while frames.try_recv().is_ok() {}
    let held = |fleet: &Fleet| {
        fleet
            .lock_connections()
            .get("gap")
            .unwrap()
            .warm_roots
            .clone()
    };
    let before = held(&fleet);
    for revision in [5, 6] {
        fleet.handle_executor_message(
            "gap",
            generation,
            ExecutorMessage::WarmRootsChanged {
                revision,
                change: WarmRootChange::Delta {
                    base_revision: revision - 1,
                    added: vec![test_warm_roots("repo", &["a9"])],
                    removed: Vec::new(),
                },
            },
        );
    }
    assert_eq!(*held(&fleet), *before, "an unmatched delta changes nothing");
    let mut requests = 0;
    while let Ok(frame) = frames.try_recv() {
        if matches!(frame, ExecutorMessage::WarmRootsRequest {}) {
            requests += 1;
        }
    }
    assert_eq!(requests, 1, "asked once, not once per delta");

    let answer = |revision| ExecutorMessage::WarmRootsChanged {
        revision,
        change: WarmRootChange::Whole {
            repositories: vec![test_warm_roots("repo", &["a1", "a9"])],
        },
    };
    fleet.handle_executor_message("gap", generation + 1, answer(9));
    assert_eq!(*held(&fleet), *before, "a retired link's change is refused");
    fleet.handle_executor_message("gap", generation, answer(7));
    let after = held(&fleet);
    assert_eq!(after.revision(), 7);
    assert!(after.holds(&test_warm_roots("repo", &[]).repository, "a9"));
    // A change older than the answer, left queued behind it, is stale.
    fleet.handle_executor_message(
        "gap",
        generation,
        ExecutorMessage::WarmRootsChanged {
            revision: 6,
            change: WarmRootChange::Whole {
                repositories: Vec::new(),
            },
        },
    );
    assert_eq!(*held(&fleet), *after);
}

#[test]
fn population_policy_never_routes_runner_local_source_to_remote_executor() {
    let connections = HashMap::from([
        fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 1, &[]),
        fleet_entry("remote", "linux", 0, &[]),
    ]);
    let mut request = targeted_request("linux");
    request.executor.as_mut().unwrap().name = Some("remote".into());
    let config = ExecutorConfig {
        project_id: "p".into(),
        project_key: "p".into(),
        default_timeout_seconds: 5,
        setup_commands: Vec::new(),
        populate: cairn_cell::PopulateConfig {
            copy: vec![".env".into()],
        },
        population_source_root: Some("/runner/checkout".into()),
    };

    assert!(require_colocated_population(&mut request, &config)
        .unwrap_err()
        .contains("local executor"));
    assert_eq!(
        choose_executor(&connections, &request)
            .unwrap()
            .unwrap()
            .executor_id,
        "remote"
    );
}

#[test]
fn the_reserved_local_name_selects_the_runner_own_executor() {
    let connections = HashMap::from([fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[])]);
    let mut request = targeted_request("linux");
    request.executor.as_mut().unwrap().name = Some(LOCAL_EXECUTOR_NAME.into());
    assert_eq!(
        choose_executor(&connections, &request)
            .unwrap()
            .unwrap()
            .executor_id,
        COLOCATED_EXECUTOR_ID
    );
}

#[test]
fn no_match_is_typed_and_never_uses_colocated() {
    let connections = HashMap::from([fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[])]);
    assert!(choose_executor(&connections, &targeted_request("windows"))
        .unwrap_err()
        .contains("no live enrolled executor"));
}

#[tokio::test]
async fn coalesced_completion_fans_out_and_restamps_public_identities() {
    let pool = Fleet::default();
    let (first_tx, first_rx) = oneshot::channel();
    let (second_tx, second_rx) = oneshot::channel();
    let first = ("first".to_string(), "attempt-1".to_string());
    let second = ("second".to_string(), "attempt-2".to_string());
    let key = CoalescingKey::new(result_identity(), first.clone());
    pool.in_flight.lock().unwrap().open(
        key.clone(),
        PlacementDemand::default(),
        HashMap::from([
            (
                first.clone(),
                CoalescedSubscriber {
                    waiter: first_tx,
                    priority: CellPriority::ReviewCheck,
                    requesting_job_id: Some("job-a".into()),
                },
            ),
            (
                second.clone(),
                CoalescedSubscriber {
                    waiter: second_tx,
                    priority: CellPriority::WriteCheck,
                    requesting_job_id: Some("job-b".into()),
                },
            ),
        ]),
    );
    pool.complete_coalesced_for_leader(
        &key,
        CellOutcome::Cancelled {
            request_id: first.0.clone(),
            attempt_id: first.1.clone(),
        },
    );
    assert_eq!(
        first_rx.await.unwrap().unwrap().outcome,
        CellOutcome::Cancelled {
            request_id: first.0,
            attempt_id: first.1,
        }
    );
    assert_eq!(
        second_rx.await.unwrap().unwrap().outcome,
        CellOutcome::Cancelled {
            request_id: second.0,
            attempt_id: second.1,
        }
    );
}

#[test]
fn result_identity_preserves_project_and_check_namespaces() {
    assert_ne!(
        CheckResultIdentity::new("project-a", "check", "input"),
        CheckResultIdentity::new("project-b", "check", "input")
    );
    assert_ne!(
        CheckResultIdentity::new("project", "check-a", "input"),
        CheckResultIdentity::new("project", "check-b", "input")
    );
}

#[test]
fn cancelling_one_subscriber_job_keeps_other_jobs_execution() {
    let pool = Fleet::default();
    let (executor_tx, mut executor_rx) = mpsc::unbounded_channel();
    let generation = pool.attach_executor(executor_tx);
    // Attaching states the runner's drain posture, so the channel is not
    // empty to begin with. What this test guards is that no *cancellation*
    // is sent while another job still wants the execution, which is a
    // different claim from the machine hearing nothing at all.
    assert!(matches!(
        executor_rx.try_recv(),
        Ok(ExecutorMessage::DrainModeRequest { enabled: false, .. })
    ));
    let first = ("first".to_string(), "attempt-1".to_string());
    let second = ("second".to_string(), "attempt-2".to_string());
    let (first_tx, _) = oneshot::channel();
    let (second_tx, _) = oneshot::channel();
    let key = CoalescingKey::new(result_identity(), first.clone());
    let mut registry = pool.in_flight.lock().unwrap();
    registry.open(
        key.clone(),
        PlacementDemand::default(),
        HashMap::from([
            (
                first.clone(),
                CoalescedSubscriber {
                    waiter: first_tx,
                    priority: CellPriority::ReviewCheck,
                    requesting_job_id: Some("job-a".into()),
                },
            ),
            (
                second.clone(),
                CoalescedSubscriber {
                    waiter: second_tx,
                    priority: CellPriority::ReviewCheck,
                    requesting_job_id: Some("job-b".into()),
                },
            ),
        ]),
    );
    drop(registry);
    pool.coalesced_leaders.lock().unwrap().insert(first.clone());
    let _pending_rx = claim_entry(
        &pool,
        &first,
        COLOCATED_EXECUTOR_ID,
        generation,
        Some("job-a"),
    );

    assert_eq!(
        pool.cancel_job_requests("job-a", CancelCause::JobStopped),
        1
    );
    assert!(executor_rx.try_recv().is_err());
    let registry = pool.in_flight.lock().unwrap();
    assert_eq!(registry.by_key[&key].subscribers.len(), 1);
    assert!(registry.by_key[&key].subscribers.contains_key(&second));
    assert!(!pool.cancelled_leaders.lock().unwrap().contains(&first));
}

#[test]
fn detaching_one_coalesced_subscriber_keeps_the_shared_execution() {
    let pool = Fleet::default();
    let first = ("first".to_string(), "attempt-1".to_string());
    let second = ("second".to_string(), "attempt-2".to_string());
    let (first_tx, _) = oneshot::channel();
    let (second_tx, _) = oneshot::channel();
    let mut registry = pool.in_flight.lock().unwrap();
    let key = CoalescingKey::new(result_identity(), first.clone());
    registry.open(
        key.clone(),
        PlacementDemand::default(),
        HashMap::from([
            (
                first.clone(),
                CoalescedSubscriber {
                    waiter: first_tx,
                    priority: CellPriority::ReviewCheck,
                    requesting_job_id: Some("job-a".into()),
                },
            ),
            (
                second.clone(),
                CoalescedSubscriber {
                    waiter: second_tx,
                    priority: CellPriority::ReviewCheck,
                    requesting_job_id: Some("job-a".into()),
                },
            ),
        ]),
    );
    drop(registry);
    pool.detach_coalesced_subscriber(&first, None);
    let registry = pool.in_flight.lock().unwrap();
    assert_eq!(registry.by_key[&key].subscribers.len(), 1);
    assert!(registry.by_key[&key].subscribers.contains_key(&second));
}

/// Admit `request` to `registry` the way both pure-verdict submission
/// paths do, as its own subscriber and would-be leader.
fn admit_request(registry: &mut InFlightRegistry, request: &CellRequest) -> (CoalescingKey, bool) {
    let identity = (request.request_id.clone(), request.attempt_id.clone());
    let (waiter, _) = oneshot::channel();
    registry.admit(
        result_identity(),
        &PlacementDemand::of(request),
        &identity,
        identity.clone(),
        CoalescedSubscriber {
            waiter,
            priority: request.priority,
            requesting_job_id: request.requesting_job_id.clone(),
        },
    )
}

fn check_request_on(id: &str, os: Option<&str>) -> CellRequest {
    let mut request = provenance_request();
    request.request_id = id.into();
    request.attempt_id = format!("{id}-attempt");
    if let Some(os) = os {
        request.executor = Some(ExecutorSelector {
            os: Some(os.into()),
            ..ExecutorSelector::default()
        });
        request.verdict_platforms = vec![os.into()];
    }
    request
}

/// `cairn check run --os linux` and `--os macos` on one commit hash to the
/// same content identity. Each must lead its own execution, so that each
/// verdict is produced under the placement its own request demanded.
#[test]
fn requests_demanding_different_platforms_lead_separate_executions() {
    let mut registry = InFlightRegistry::default();
    let linux = check_request_on("linux", Some("linux"));
    let macos = check_request_on("macos", Some("macos"));
    let (linux_key, linux_opened) = admit_request(&mut registry, &linux);
    let (macos_key, macos_opened) = admit_request(&mut registry, &macos);

    assert!(linux_opened && macos_opened, "neither joins the other");
    assert_ne!(linux_key, macos_key);
    assert_eq!(linux_key.result, macos_key.result);
    assert_eq!(registry.by_key.len(), 2);
    for (key, request) in [(&linux_key, &linux), (&macos_key, &macos)] {
        let execution = &registry.by_key[key];
        assert_eq!(
            key.leader,
            (request.request_id.clone(), request.attempt_id.clone()),
            "each request leads the execution it waits on"
        );
        assert_eq!(execution.demand, PlacementDemand::of(request));
        assert_eq!(execution.subscribers.len(), 1);
    }

    // A later request for either platform joins the execution that
    // demanded it, never the other one.
    let (joined, opened) =
        admit_request(&mut registry, &check_request_on("macos-2", Some("macos")));
    assert!(!opened);
    assert_eq!(joined, macos_key);
}

#[tokio::test]
async fn shared_publication_registration_allows_pending_retry_but_not_settled_movement() {
    let db = crate::storage::migrated_test_db("publication-registration.db").await;
    db.execute_script(
        "INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
             VALUES('p','default','P','pub','/unused',1,1);
             INSERT INTO check_requests(id,project_id,accepted_at_ms,suite,consumer,requester_ref,priority,state,state_since_ms,updated_ms)
             VALUES('pending','p',1,'smoke','requested','test','review_check','queued',1,1),('receipt','p',1,'smoke','requested','test','review_check','queued',1,1),('attached','p',1,'smoke','requested','test','review_check','queued',1,1);
             INSERT INTO check_result_observations(
               id,project_id,commit_sha,tree_hash,check_name,input_hash,environment_fingerprint,
               exit_code,verdict,complete,reusable,parser_version,result_schema_version,
               ran_at,duration_ms,cadence,output_tail)
             VALUES('source','p','commit','tree','suite','input','env',0,'passed',1,1,1,1,1,1,'review','ok');
             UPDATE check_requests SET outcome_json='{}',publication_id='receipt-group' WHERE id='receipt';
             UPDATE check_requests SET observation_id='source',publication_id='attached-group' WHERE id='attached';"
    ).await.unwrap();
    let first = PublicationCoordination::new();
    let retry = PublicationCoordination::new();
    for publication in [&first, &retry] {
        register_manual_check_publications(
            &db,
            "p".into(),
            vec![("pending".into(), publication.id().into())],
        )
        .await
        .unwrap();
    }
    let registered = db
        .query_one(
            "SELECT publication_id FROM check_requests WHERE id='pending'",
            (),
            |row| row.text(0),
        )
        .await
        .unwrap();
    assert_eq!(registered, retry.id());
    for handle in ["receipt", "attached"] {
        assert!(register_manual_check_publications(
            &db,
            "p".into(),
            vec![(handle.into(), retry.id().into())]
        )
        .await
        .is_err());
    }
    let bindings = db.query_all(
        "SELECT publication_id FROM check_requests WHERE id IN ('receipt','attached') ORDER BY id",
        (), |row| row.text(0),
    ).await.unwrap();
    assert_eq!(bindings, vec!["attached-group", "receipt-group"]);
    // A malformed batch cannot partially move the first pending handle.
    assert!(register_manual_check_publications(
        &db,
        "p".into(),
        vec![
            ("pending".into(), first.id().into()),
            ("missing".into(), first.id().into()),
        ]
    )
    .await
    .is_err());
    assert_eq!(
        db.query_one(
            "SELECT publication_id FROM check_requests WHERE id='pending'",
            (),
            |row| row.text(0),
        )
        .await
        .unwrap(),
        retry.id()
    );
}

#[test]
fn shared_publication_identity_tracks_the_actual_execution_not_its_content() {
    let mut registry = InFlightRegistry::default();
    let linux = check_request_on("linux-leader", Some("linux"));
    let (linux_key, _) = admit_request(&mut registry, &linux);
    let linux_publication = registry.by_key[&linux_key].publication.clone();
    uuid::Uuid::parse_str(linux_publication.id()).unwrap();
    let (joined, opened) = admit_request(
        &mut registry,
        &check_request_on("linux-follower", Some("linux")),
    );
    assert!(!opened);
    assert_eq!(
        linux_publication.id(),
        registry.by_key[&joined].publication.id()
    );
    let (macos_key, opened) = admit_request(
        &mut registry,
        &check_request_on("macos-leader", Some("macos")),
    );
    assert!(opened);
    assert_eq!(linux_key.result, macos_key.result);
    assert_ne!(
        linux_publication.id(),
        registry.by_key[&macos_key].publication.id()
    );
    registry.by_key.remove(&linux_key);
    let (retry_key, opened) = admit_request(&mut registry, &linux);
    assert!(opened);
    assert_ne!(
        linux_publication.id(),
        registry.by_key[&retry_key].publication.id(),
        "even an identical request retry is a new publication"
    );
}

/// The existing behaviour stays: requests without a demand share one
/// execution, and a request without a demand may join one that has one.
#[test]
fn requests_without_a_platform_demand_still_coalesce() {
    let mut registry = InFlightRegistry::default();
    let (first_key, first_opened) = admit_request(&mut registry, &check_request_on("first", None));
    let (second_key, second_opened) =
        admit_request(&mut registry, &check_request_on("second", None));
    assert!(first_opened);
    assert!(!second_opened);
    assert_eq!(first_key, second_key);
    assert_eq!(registry.by_key.len(), 1);
    assert_eq!(registry.by_key[&first_key].subscribers.len(), 2);

    // A linux demand cannot ride the unconstrained leader, but an
    // unconstrained request may still ride a linux one.
    let mut pinned_first = InFlightRegistry::default();
    let (linux_key, _) =
        admit_request(&mut pinned_first, &check_request_on("linux", Some("linux")));
    let (anywhere_key, anywhere_opened) =
        admit_request(&mut pinned_first, &check_request_on("anywhere", None));
    assert!(!anywhere_opened);
    assert_eq!(anywhere_key, linux_key);
    let (_, linux_opened) = admit_request(&mut registry, &check_request_on("linux", Some("linux")));
    assert!(linux_opened);
}

/// Register one coalesced execution led by `leader`, with each of
/// `subscribers` waiting on it for its own job.
fn register_coalesced(
    pool: &Fleet,
    leader: &RequestIdentity,
    subscribers: Vec<(RequestIdentity, oneshot::Sender<CoalescedDelivery>, &str)>,
) -> CoalescingKey {
    let key = CoalescingKey::new(result_identity(), leader.clone());
    let mut registry = pool.in_flight.lock().unwrap();
    let mut waiting = HashMap::new();
    for (identity, waiter, job) in subscribers {
        registry
            .subscriber_keys
            .insert(identity.clone(), key.clone());
        waiting.insert(
            identity,
            CoalescedSubscriber {
                waiter,
                priority: CellPriority::ReviewCheck,
                requesting_job_id: Some(job.into()),
            },
        );
    }
    registry.open(key.clone(), PlacementDemand::default(), waiting);
    drop(registry);
    pool.coalesced_leaders
        .lock()
        .unwrap()
        .insert(leader.clone());
    key
}

/// A compaction resets the session, not the commit: of three waits in the
/// job on one shared answer, its job-wide cancel ends only the session's
/// own run batch, and leaves the turn-end wave's and the write-cadence
/// lane's to finish. Stopping the job still ends both.
#[tokio::test]
async fn a_compaction_spares_the_jobs_review_priority_work() {
    let pool = Fleet::default();
    let wave = ("wave".to_string(), "attempt-w".to_string());
    let lane = ("lane".to_string(), "attempt-l".to_string());
    let batch = ("batch".to_string(), "attempt-b".to_string());
    let (wave_tx, wave_rx) = oneshot::channel();
    let key = register_coalesced(&pool, &wave, vec![(wave.clone(), wave_tx, "job")]);
    let (lane_tx, lane_rx) = oneshot::channel();
    let (batch_tx, batch_rx) = oneshot::channel();
    {
        let mut registry = pool.in_flight.lock().unwrap();
        for (identity, waiter, priority) in [
            (lane.clone(), lane_tx, CellPriority::WriteCheck),
            (batch.clone(), batch_tx, CellPriority::AgentInteractive),
        ] {
            registry
                .subscriber_keys
                .insert(identity.clone(), key.clone());
            registry.by_key.get_mut(&key).unwrap().subscribers.insert(
                identity,
                CoalescedSubscriber {
                    waiter,
                    priority,
                    requesting_job_id: Some("job".into()),
                },
            );
        }
    }

    assert_eq!(
        pool.cancel_job_requests("job", CancelCause::OperatorCompact),
        1,
        "only the session's own run batch is cancelled"
    );
    assert_eq!(
        batch_rx.await.unwrap().err(),
        Some(CoalescedWaitError::Cancelled(CancelCause::OperatorCompact))
    );
    {
        let registry = pool.in_flight.lock().unwrap();
        let subscribers = &registry.by_key[&key].subscribers;
        assert!(
            subscribers.contains_key(&wave),
            "the wave still waits on the execution"
        );
        assert!(
            subscribers.contains_key(&lane),
            "the write-cadence lane still waits on the execution"
        );
    }
    assert!(!pool.cancelled_leaders.lock().unwrap().contains(&wave));

    assert_eq!(pool.cancel_job_requests("job", CancelCause::JobStopped), 2);
    assert_eq!(
        wave_rx.await.unwrap().err(),
        Some(CoalescedWaitError::Cancelled(CancelCause::JobStopped))
    );
    assert_eq!(
        lane_rx.await.unwrap().err(),
        Some(CoalescedWaitError::Cancelled(CancelCause::JobStopped))
    );
}

/// The same rule on the queue: a compaction sends `Cancel` for the job's
/// claimed run batch and not for its claimed review or write check.
#[test]
fn a_compaction_cancels_the_jobs_claimed_batch_and_not_its_review_check() {
    let pool = Fleet::default();
    let (executor_tx, mut executor_rx) = mpsc::unbounded_channel();
    let generation = pool.attach_executor(executor_tx);
    while executor_rx.try_recv().is_ok() {}
    let claim = |key: &RequestIdentity, priority: CellPriority| {
        let mut request = provenance_request();
        request.request_id.clone_from(&key.0);
        request.attempt_id.clone_from(&key.1);
        request.requesting_job_id = Some("job".into());
        request.priority = priority;
        request.wait_horizon_unix_ms = unix_time_ms() + 60_000;
        let (entry_id, receiver) = pool
            .queue
            .enqueue(
                queue::NewEntry {
                    request,
                    admission_kind: CellAdmissionKind::Command,
                    interactive: false,
                    execution_residency: None,
                    standing: false,
                },
                unix_time_ms(),
            )
            .expect("a fresh request identity enqueues");
        assert!(pool.queue.offer(
            &entry_id,
            COLOCATED_EXECUTOR_ID,
            COLOCATED_EXECUTOR_ID,
            generation,
            unix_time_ms()
        ));
        assert_eq!(
            pool.queue
                .claim(&entry_id, COLOCATED_EXECUTOR_ID, generation, unix_time_ms()),
            queue::ClaimVerdict::Granted
        );
        receiver
    };
    let batch = ("batch".to_string(), "attempt-b".to_string());
    let review = ("review".to_string(), "attempt-r".to_string());
    let _batch_rx = claim(&batch, CellPriority::AgentInteractive);
    let _review_rx = claim(&review, CellPriority::ReviewCheck);
    let write = ("write".to_string(), "attempt-w".to_string());
    let _write_rx = claim(&write, CellPriority::WriteCheck);

    assert_eq!(
        pool.cancel_job_requests("job", CancelCause::OperatorCompact),
        1
    );
    let mut cancelled = Vec::new();
    while let Ok(message) = executor_rx.try_recv() {
        if let ExecutorMessage::Cancel {
            request_id,
            attempt_id,
        } = message
        {
            cancelled.push((request_id, attempt_id));
        }
    }
    assert_eq!(cancelled, vec![batch]);
}

/// A turn-end wave abandons its lanes by dropping the future awaiting
/// them. Another consumer in the same job waiting on the same answer — a
/// manual check, a merge gate — must still receive it.
#[tokio::test]
async fn dropping_one_consumers_wait_leaves_a_same_job_sibling_its_result() {
    let pool = Fleet::default();
    let wave = ("wave".to_string(), "attempt-1".to_string());
    let manual = ("manual".to_string(), "attempt-2".to_string());
    let (wave_tx, wave_rx) = oneshot::channel();
    let (manual_tx, manual_rx) = oneshot::channel();
    let key = register_coalesced(
        &pool,
        &wave,
        vec![
            (wave.clone(), wave_tx, "job"),
            (manual.clone(), manual_tx, "job"),
        ],
    );

    let wave_wait = pool.await_coalesced(wave.clone(), unix_time_ms() + 30_000, wave_rx);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), wave_wait)
            .await
            .is_err(),
        "nothing has published yet"
    );

    {
        let registry = pool.in_flight.lock().unwrap();
        let execution = &registry.by_key[&key];
        assert_eq!(execution.subscribers.len(), 1);
        assert!(execution.subscribers.contains_key(&manual));
    }
    assert!(!pool.cancelled_leaders.lock().unwrap().contains(&wave));

    let mut request = provenance_request();
    request.request_id.clone_from(&wave.0);
    request.attempt_id.clone_from(&wave.1);
    pool.complete_coalesced_for_leader(&key, completed_for(&request, COLOCATED_EXECUTOR_ID, 1));
    let delivered = manual_rx
        .await
        .expect("the sibling's waiter is answered")
        .expect("the sibling receives the execution's outcome");
    assert!(matches!(
        delivered.outcome,
        CellOutcome::Completed { ref request_id, .. } if request_id == &manual.0
    ));
}

/// A job stop ends a lane that is still waiting. Its waiter is told the
/// stop, never handed a closed channel it would read as a lost machine.
#[tokio::test]
async fn a_job_stop_tells_its_waiting_subscriber_why() {
    let pool = Fleet::default();
    let lane = ("lane".to_string(), "attempt-1".to_string());
    let (tx, rx) = oneshot::channel();
    register_coalesced(&pool, &lane, vec![(lane.clone(), tx, "job")]);
    let waiting = tokio::spawn({
        let pool = pool.clone();
        let lane = lane.clone();
        async move {
            pool.await_coalesced(lane, unix_time_ms() + 30_000, rx)
                .await
        }
    });
    tokio::task::yield_now().await;

    assert_eq!(pool.cancel_job_requests("job", CancelCause::JobStopped), 1);
    let delivered = tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .expect("the waiter is answered at once")
        .unwrap();
    assert_eq!(
        delivered.err(),
        Some(CoalescedWaitError::Cancelled(CancelCause::JobStopped))
    );
}

/// Another job's stop releases only that job's interest: a follower in a
/// different job keeps waiting and receives the leader's result.
#[tokio::test]
async fn a_job_stop_leaves_another_jobs_follower_its_result() {
    let pool = Fleet::default();
    let leader = ("leader".to_string(), "attempt-a".to_string());
    let follower = ("follower".to_string(), "attempt-b".to_string());
    let (leader_tx, leader_rx) = oneshot::channel();
    let (follower_tx, follower_rx) = oneshot::channel();
    let key = register_coalesced(
        &pool,
        &leader,
        vec![
            (leader.clone(), leader_tx, "job-a"),
            (follower.clone(), follower_tx, "job-b"),
        ],
    );

    pool.cancel_job_requests("job-a", CancelCause::JobStopped);
    assert_eq!(
        leader_rx.await.unwrap().err(),
        Some(CoalescedWaitError::Cancelled(CancelCause::JobStopped))
    );
    assert!(!pool.cancelled_leaders.lock().unwrap().contains(&leader));

    let mut request = provenance_request();
    request.request_id.clone_from(&leader.0);
    request.attempt_id.clone_from(&leader.1);
    pool.complete_coalesced_for_leader(&key, completed_for(&request, COLOCATED_EXECUTOR_ID, 1));
    let delivered = follower_rx
        .await
        .unwrap()
        .expect("the follower gets the result");
    assert!(matches!(
        delivered.outcome,
        CellOutcome::Completed { ref request_id, .. } if request_id == &follower.0
    ));
}

/// A job stop that reaches a request outside any coalesced execution sends
/// the machine `Cancel` and keeps why, for the outcome that comes back.
#[test]
fn a_job_stop_records_its_cause_for_a_dispatched_request() {
    let pool = Fleet::default();
    let (executor_tx, _executor_rx) = mpsc::unbounded_channel();
    let generation = pool.attach_executor(executor_tx);
    let request = ("request".to_string(), "attempt".to_string());
    let _pending = claim_entry(
        &pool,
        &request,
        COLOCATED_EXECUTOR_ID,
        generation,
        Some("job"),
    );

    assert_eq!(
        pool.cancel_job_requests("job", CancelCause::SessionClosed),
        1
    );
    assert_eq!(
        pool.cancel_causes.lock().unwrap().take(&request),
        Some(CancelCause::SessionClosed)
    );
}

/// Work answered because its machine's link ended says which machine and
/// which generation, so a reader can tell a real link loss from anything else.
#[tokio::test]
async fn a_link_loss_answer_names_the_machine_and_generation() {
    let pool = Fleet::default();
    let (executor_tx, _executor_rx) = mpsc::unbounded_channel();
    let generation = pool.attach_executor(executor_tx);
    let request = ("request".to_string(), "attempt".to_string());
    let pending = claim_entry(&pool, &request, COLOCATED_EXECUTOR_ID, generation, None);

    pool.queue.fail_claims_for(
        COLOCATED_EXECUTOR_ID,
        "executor connection closed before returning a result",
        unix_time_ms(),
    );
    let CellOutcome::Unavailable { diagnostic, .. } = pending.await.unwrap() else {
        panic!("a lost link answers unavailable");
    };
    assert!(
        diagnostic.starts_with(&format!(
            "executor {COLOCATED_EXECUTOR_ID} (generation {generation}): "
        )),
        "{diagnostic}"
    );
}

/// Cancelling the last consumer of a shared execution stops it, but the
/// execution stays joinable until its terminal outcome. A follower that
/// joins in that window learns the real cause, not "cause not recorded".
#[tokio::test]
async fn a_follower_joining_a_cancelled_execution_learns_why() {
    let pool = Fleet::default();
    let leader = ("leader".to_string(), "attempt-a".to_string());
    let (leader_tx, _leader_rx) = oneshot::channel();
    let key = register_coalesced(&pool, &leader, vec![(leader.clone(), leader_tx, "job")]);
    pool.cancel_job_requests("job", CancelCause::JobStopped);

    let follower = ("follower".to_string(), "attempt-b".to_string());
    let (follower_tx, follower_rx) = oneshot::channel();
    {
        let mut registry = pool.in_flight.lock().unwrap();
        registry
            .by_key
            .get_mut(&key)
            .expect("the cancelled execution remains joinable")
            .subscribers
            .insert(
                follower.clone(),
                CoalescedSubscriber {
                    waiter: follower_tx,
                    priority: CellPriority::ReviewCheck,
                    requesting_job_id: Some("other-job".into()),
                },
            );
        registry
            .subscriber_keys
            .insert(follower.clone(), key.clone());
    }

    // What the leader's task does when its cancelled outcome comes back.
    let account = CellAccount {
        left_running: None,
        cancel_cause: pool.cancel_causes.lock().unwrap().take(&leader),
        placement: None,
    };
    pool.complete_coalesced_for_leader(
        &key,
        LeaderSettlement::Settled {
            outcome: CellOutcome::Cancelled {
                request_id: leader.0.clone(),
                attempt_id: leader.1.clone(),
            },
            account,
        },
    );
    let delivered = follower_rx
        .await
        .unwrap()
        .expect("the follower gets the outcome");
    assert!(matches!(delivered.outcome, CellOutcome::Cancelled { .. }));
    assert_eq!(
        delivered.account.cancel_cause,
        Some(CancelCause::JobStopped)
    );
}

/// A claim carried across a reconnect is answered for by its new
/// generation, and that is the generation its caller collects.
#[tokio::test]
async fn the_settling_binding_is_the_rebound_generation() {
    let pool = Fleet::default();
    let (executor_tx, _executor_rx) = mpsc::unbounded_channel();
    let claimed_on = pool.attach_executor(executor_tx);
    let request = ("request".to_string(), "attempt".to_string());
    let pending = claim_entry(&pool, &request, COLOCATED_EXECUTOR_ID, claimed_on, None);
    let rebound_to = claimed_on + 1;
    let carried = pool.queue.rebind(COLOCATED_EXECUTOR_ID, rebound_to);
    assert!(
        carried.contains_key(&request),
        "the claim is carried across"
    );

    pool.queue.fail_claims_for(
        COLOCATED_EXECUTOR_ID,
        "executor connection closed before returning a result",
        unix_time_ms(),
    );
    let CellOutcome::Unavailable { diagnostic, .. } = pending.await.unwrap() else {
        panic!("a lost link answers unavailable");
    };
    assert!(
        diagnostic.contains(&format!("(generation {rebound_to})")),
        "{diagnostic}"
    );
    assert_eq!(
        pool.queue.take_settled_binding(&request),
        Some((COLOCATED_EXECUTOR_ID.to_string(), rebound_to))
    );
}

#[test]
fn the_first_cancel_cause_stands_and_the_ledger_stays_bounded() {
    let mut causes = CancelCauses::default();
    let first = ("first".to_string(), "attempt".to_string());
    causes.record(first.clone(), CancelCause::Superseded);
    causes.record(first.clone(), CancelCause::JobStopped);
    assert_eq!(causes.take(&first), Some(CancelCause::Superseded));
    assert_eq!(causes.take(&first), None);

    for index in 0..=RECENT_CANCEL_CAUSES {
        causes.record((index.to_string(), "a".into()), CancelCause::JobStopped);
    }
    assert_eq!(causes.causes.len(), RECENT_CANCEL_CAUSES);
    assert_eq!(causes.take(&("0".to_string(), "a".to_string())), None);
}

#[test]
fn abandoning_last_subscriber_keeps_the_leader_coalescible_until_terminal_outcome() {
    let pool = Fleet::default();
    let (executor_tx, mut executor_rx) = mpsc::unbounded_channel();
    let generation = pool.attach_executor(executor_tx);
    // Attaching states the runner's drain posture on every connection, so
    // that frame is already queued ahead of anything this test provokes.
    // What is asserted below is that the leader is told to cancel, never
    // that the cancel is the first thing the machine ever hears.
    assert!(matches!(
        executor_rx.try_recv(),
        Ok(ExecutorMessage::DrainModeRequest { enabled: false, .. })
    ));
    let leader = ("leader".to_string(), "attempt-1".to_string());
    let resubmitted = ("resubmitted".to_string(), "attempt-2".to_string());
    let key = CoalescingKey::new(result_identity(), leader.clone());
    let (first_tx, _first_rx) = oneshot::channel();
    {
        let mut registry = pool.in_flight.lock().unwrap();
        registry.open(
            key.clone(),
            PlacementDemand::default(),
            HashMap::from([(
                leader.clone(),
                CoalescedSubscriber {
                    waiter: first_tx,
                    priority: CellPriority::ReviewCheck,
                    requesting_job_id: Some("job".into()),
                },
            )]),
        );
    }
    let _entry = claim_entry(
        &pool,
        &leader,
        COLOCATED_EXECUTOR_ID,
        generation,
        Some("job"),
    );

    pool.detach_coalesced_subscriber(&leader, None);
    assert!(matches!(
        executor_rx.try_recv(),
        Ok(ExecutorMessage::Cancel { ref request_id, ref attempt_id })
            if request_id == &leader.0 && attempt_id == &leader.1
    ));

    let (resubmit_tx, _resubmit_rx) = oneshot::channel();
    let mut registry = pool.in_flight.lock().unwrap();
    let (joined, opened) = registry.admit(
        result_identity(),
        &PlacementDemand::default(),
        &resubmitted,
        resubmitted.clone(),
        CoalescedSubscriber {
            waiter: resubmit_tx,
            priority: CellPriority::ReviewCheck,
            requesting_job_id: Some("job".into()),
        },
    );
    assert!(
        !opened,
        "the cancelling leader remains the coalescing authority"
    );
    assert_eq!(joined, key);
}

fn register_held_subscriber(
    pool: &Fleet,
    generation: u64,
    identity: RequestIdentity,
    waiter: oneshot::Sender<CoalescedDelivery>,
    state: ExecutorSubstrateState,
) -> CoalescingKey {
    let key = CoalescingKey::new(result_identity(), identity.clone());
    pool.in_flight.lock().unwrap().open(
        key.clone(),
        PlacementDemand::default(),
        HashMap::from([(
            identity.clone(),
            CoalescedSubscriber {
                waiter,
                priority: CellPriority::ReviewCheck,
                requesting_job_id: Some("job".into()),
            },
        )]),
    );
    let _pending_rx = claim_entry(
        pool,
        &identity,
        COLOCATED_EXECUTOR_ID,
        generation,
        Some("job"),
    );
    let now = unix_time_ms();
    pool.set_executor_snapshot(
        COLOCATED_EXECUTOR_ID,
        generation,
        FleetSnapshot {
            queued_requests: vec![QueuedCellRequest {
                command_resource_identity: None,
                executor_id: COLOCATED_EXECUTOR_ID.into(),
                request_id: identity.0,
                attempt_id: identity.1,
                project_id: "p".into(),
                command: "check".into(),
                command_class: cairn_common::executor_protocol::CellCommandClass::Other,
                owner: None,
                priority: CellPriority::ReviewCheck,
                effective_priority: Some(CellPriority::ReviewCheck),
                requesting_job_id: Some("job".into()),
                affinity_key: None,
                queued_at_unix_ms: now,
                resource_reservation: Default::default(),
                learned_estimate: None,
                admission_kind: CellAdmissionKind::Command,
                subscriber_count: 1,
                substrate_hold: Some(ExecutorSubstrateEvidence {
                    state,
                    since_unix_ms: now,
                    last_progress_unix_ms: now,
                    diagnostic: None,
                    queue_depth: Some(3),
                    queue_position: Some(2),
                    active_cell_count: Some(2),
                    oldest_running_started_at_unix_ms: Some(now.saturating_sub(50)),
                }),
                placement: None,
            }],
            ..FleetSnapshot::default()
        },
        ExecutorSubstrateReport::default(),
    );
    key
}

#[tokio::test]
async fn a_subscriber_waits_out_capacity_contention_until_its_leader_completes() {
    let pool = Fleet::default();
    let (executor_tx, mut executor_rx) = mpsc::unbounded_channel();
    let generation = pool.attach_executor(executor_tx);
    assert!(matches!(
        executor_rx.recv().await,
        Some(ExecutorMessage::DrainModeRequest { enabled: false, .. })
    ));
    let identity = ("leader".to_string(), "attempt".to_string());
    let (tx, rx) = oneshot::channel();
    let key = register_held_subscriber(
        &pool,
        generation,
        identity.clone(),
        tx,
        ExecutorSubstrateState::CapacityBusy,
    );
    let completion_pool = pool.clone();
    let completed_identity = identity.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        completion_pool.complete_coalesced_for_leader(
            &key,
            CellOutcome::Cancelled {
                request_id: completed_identity.0.clone(),
                attempt_id: completed_identity.1.clone(),
            },
        );
    });
    let outcome = pool
        .await_coalesced(identity.clone(), unix_time_ms() + 30_000, rx)
        .await
        .expect("a subscriber must outlive capacity contention it declared patience for");
    assert!(matches!(outcome.outcome, CellOutcome::Cancelled { .. }));
    assert!(matches!(
        executor_rx.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn a_subscriber_waits_out_slot_adoption() {
    let pool = Fleet::default();
    let (executor_tx, mut executor_rx) = mpsc::unbounded_channel();
    let generation = pool.attach_executor(executor_tx);
    assert!(matches!(
        executor_rx.recv().await,
        Some(ExecutorMessage::DrainModeRequest { enabled: false, .. })
    ));
    let identity = ("leader".to_string(), "attempt".to_string());
    let (tx, rx) = oneshot::channel();
    let key = register_held_subscriber(
        &pool,
        generation,
        identity.clone(),
        tx,
        ExecutorSubstrateState::SlotAdoption,
    );
    let completion_pool = pool.clone();
    let completed_identity = identity.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        completion_pool.complete_coalesced_for_leader(
            &key,
            CellOutcome::Cancelled {
                request_id: completed_identity.0.clone(),
                attempt_id: completed_identity.1.clone(),
            },
        );
    });

    let outcome = pool
        .await_coalesced(identity, unix_time_ms() + 30_000, rx)
        .await
        .expect("a subscriber must outlive a cell adoption it declared patience for");
    assert!(matches!(outcome.outcome, CellOutcome::Cancelled { .. }));
    assert!(matches!(
        executor_rx.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn coalesced_subscriber_returns_typed_stall_with_queue_facts() {
    let pool = Fleet::default();
    let (executor_tx, _executor_rx) = mpsc::unbounded_channel();
    let generation = pool.attach_executor(executor_tx);
    let identity = ("leader".to_string(), "attempt".to_string());
    let (tx, rx) = oneshot::channel();
    register_held_subscriber(
        &pool,
        generation,
        identity.clone(),
        tx,
        ExecutorSubstrateState::CapacityBusy,
    );
    pool.lock_connections()
        .get_mut(COLOCATED_EXECUTOR_ID)
        .unwrap()
        .last_progress_unix_ms = 0;

    let outcome = match pool
        .await_coalesced(identity, unix_time_ms() + 20, rx)
        .await
    {
        Ok(_) => panic!("stalled subscriber unexpectedly completed"),
        Err(CoalescedWaitError::Refused(outcome)) => *outcome,
        Err(error) => panic!("a stalled subscriber is refused at its deadline, got {error:?}"),
    };
    assert!(matches!(
        outcome,
        CellOutcome::Unavailable {
            reason: CellUnavailableReason::Deadline {
                substrate: Some(ExecutorSubstrateEvidence {
                    state: ExecutorSubstrateState::ConnectedStalled,
                    queue_depth: Some(3),
                    queue_position: Some(2),
                    active_cell_count: Some(2),
                    ..
                }),
                ..
            },
            ..
        }
    ));
}

#[test]
fn ownerless_preparing_leader_rewrites_stale_capacity_as_connected_stalled() {
    let pool = Fleet::default();
    let leader = ("leader".to_string(), "attempt".to_string());
    let key = CoalescingKey::new(result_identity(), leader.clone());
    let (tx, _rx) = oneshot::channel();
    {
        let mut registry = pool.in_flight.lock().unwrap();
        registry.open(
            key,
            PlacementDemand::default(),
            HashMap::from([(
                leader.clone(),
                CoalescedSubscriber {
                    waiter: tx,
                    priority: CellPriority::ReviewCheck,
                    requesting_job_id: Some("job".into()),
                },
            )]),
        );
    }
    let last_progress_unix_ms = unix_time_ms().saturating_sub(EXECUTOR_PROGRESS_FRESHNESS_MS + 1);
    *pool.colocated_substrate_state.lock().unwrap() = Some(ExecutorSubstrateEvidence {
        state: ExecutorSubstrateState::CapacityBusy,
        since_unix_ms: last_progress_unix_ms.saturating_sub(10),
        last_progress_unix_ms,
        diagnostic: None,
        queue_depth: Some(3),
        queue_position: Some(2),
        active_cell_count: Some(2),
        oldest_running_started_at_unix_ms: Some(last_progress_unix_ms.saturating_sub(50)),
    });

    let evidence = pool.leader_deadline_evidence(&leader);
    assert_eq!(evidence.state, ExecutorSubstrateState::ConnectedStalled);
    assert_eq!(evidence.since_unix_ms, last_progress_unix_ms);
    assert_eq!(evidence.last_progress_unix_ms, last_progress_unix_ms);
    assert_eq!(evidence.queue_depth, Some(3));
    assert_eq!(evidence.queue_position, Some(2));
    assert_eq!(evidence.active_cell_count, Some(2));
}

#[tokio::test]
async fn dropped_publication_guard_transfers_ownership() {
    let coordination = PublicationCoordination::new();
    let PublicationRole::Publisher(first) = coordination.acquire().await else {
        panic!("first subscriber should publish");
    };
    drop(first);
    let PublicationRole::Publisher(second) = coordination.acquire().await else {
        panic!("publication ownership should transfer");
    };
    let recorded = crate::execution::cache::RecordedCheckObservation {
        id: "obs-published".to_string(),
        public_handle: "111111111111111111111111".into(),
        ran_at: 1,
        environment_fingerprint: "env".to_string(),
        executor_id: None,
        reusable: true,
    };
    second.published(Some(recorded.clone()));
    let PublicationRole::Published(carried) = coordination.acquire().await else {
        panic!("a published verdict must be reported as published");
    };
    assert_eq!(
        carried,
        Some(recorded),
        "a coalesced sibling must learn which observation the publisher wrote"
    );
}

#[test]
fn mismatched_terminal_identity_is_rejected() {
    let outcome = CellOutcome::Cancelled {
        request_id: "r".into(),
        attempt_id: "old".into(),
    };
    assert!(!outcome_matches(&outcome, "r", "new"));
}
#[test]
fn adopted_executor_lost_process_is_reconciled_to_lifetime_subscribers() {
    let pool = Fleet::default();
    let (executor_tx, _executor_rx) = mpsc::unbounded_channel();
    let generation = pool.attach_executor(executor_tx);
    let received = Arc::new(Mutex::new(Vec::new()));
    let captured = received.clone();
    pool.subscribe_resident_process_events(move |event| {
        captured.lock().unwrap().push(event);
    })
    .detach();
    let residency = fleet_residency(
        ResidencyHolder::Job {
            job_id: "job".into(),
        },
        None,
    );
    let status = cairn_common::executor_protocol::ResidentProcessStatus::Exited {
        finished_at_unix_ms: 42,
        exit_code: None,
        restartable: true,
        executor_lost: true,
    };
    let cell = resident_process_cell(
        CellResidency {
            phase: cairn_common::executor_protocol::ResidencyPhase::AwaitingReclaim,
            reclaim_deadline_unix_ms: 100,
            state_revision: 2,
            ..residency.clone()
        },
        7,
        3,
        status.clone(),
    );

    assert!(pool.set_executor_snapshot(
        COLOCATED_EXECUTOR_ID,
        generation,
        FleetSnapshot {
            cells: vec![cell],
            ..Default::default()
        },
        ExecutorSubstrateReport::default(),
    ));
    assert_eq!(
        received.lock().unwrap().as_slice(),
        &[ResidentProcessEvent {
            holder: residency.holder.clone(),
            incarnation_id: "incarnation".into(),
            cell_epoch: 7,
            process_key: "main".into(),
            process_generation: 3,
            event: ResidentProcessEventKind::State { status },
        }]
    );
}

/// A cell holding one resident process, at a stated cell epoch and process
/// generation.
fn resident_process_cell(
    residency: CellResidency,
    cell_epoch: u64,
    process_generation: u64,
    status: cairn_common::executor_protocol::ResidentProcessStatus,
) -> PersistentCellState {
    PersistentCellState {
        observed_head: None,
        size_bytes: 0,
        warm_command_classes: Vec::new(),
        executor_id: String::new(),
        executor_display_name: None,
        project_id: "p".into(),
        cell_id: "slot".into(),
        path: "/slot".into(),
        workspace_name: "slot".into(),
        repository: "/repo".into(),
        checkout_kind: Default::default(),
        git_common_dir: None,
        authority_path: "/slot/.authority".into(),
        lifecycle: PersistentCellLifecycle::Active,
        cell_epoch,
        last_sealed_commit: Some("base".into()),
        last_distance: None,
        quarantine_reason: None,
        last_used_unix_ms: 42,
        last_affinity_key: None,
        preparation_fingerprint: None,
        residency: Some(residency),
        occupancy: CellOccupancy {
            command: None,
            command_ownership: None,
            draining: None,
            processes: std::collections::BTreeMap::from([(
                "main".into(),
                cairn_common::executor_protocol::ResidentProcess {
                    generation: process_generation,
                    kind: cairn_common::executor_protocol::ResidentProcessKind::Terminal {
                        slug: "watch".into(),
                    },
                    spec: None,
                    status,
                    reservation: None,
                    live_usage: None,
                    charge: None,
                },
            )]),
            clone_source: None,
        },
        seeded_from: None,
    }
}

// ── Ingested output is scrubbed against this runner's registry ─────────
//
// Terminals, REPLs, workflows and dev instances are each given a
// per-process relay capability this runner mints: kept by the executor and
// attached on the lineage socket on unix, carried in `CAIRN_MCP_SECRET` only
// on Windows. The executor's scrubber does not register it; this runner's
// does. These cover the pass that closes that gap.

const RESIDENT_SECRET: &str = "resident-Qa9Zm2Xp7Lr4";

fn register_resident_secret(id: &str) -> crate::security::SecretGuard<'static> {
    use crate::security::{registry, SecretCategory, SecretId, SecretMaterial};
    registry()
        .register(
            SecretId::new(id),
            SecretCategory::CallbackCredential,
            "unit test",
            SecretMaterial::from_string(RESIDENT_SECRET.to_string()),
        )
        .expect("registerable")
}

/// Collect what reaches resident-process subscribers, in order.
fn subscribed_events(pool: &Fleet) -> Arc<Mutex<Vec<ResidentProcessEvent>>> {
    let received = Arc::new(Mutex::new(Vec::new()));
    let captured = received.clone();
    pool.subscribe_resident_process_events(move |event| {
        captured.lock().unwrap().push(event);
    })
    .detach();
    received
}

/// One stdout chunk from the service fixture's process, as an executor
/// would send it.
fn service_output(residency: &CellResidency) -> ExecutorMessage {
    ExecutorMessage::ResidentProcessEvent {
        event: ResidentProcessEvent {
            holder: residency.holder.clone(),
            incarnation_id: residency.incarnation_id.clone(),
            cell_epoch: 7,
            process_key: "main".into(),
            process_generation: 2,
            event: ResidentProcessEventKind::Output {
                sequence: 1,
                stream: cairn_common::executor_protocol::ResidentProcessStream::Stdout,
                data: b"guid".to_vec(),
            },
        },
    }
}

/// A per-process subscriber leaves with its owner instead of accumulating
/// for the runner's life, while a detached one stays.
#[test]
fn a_dropped_resident_subscription_hears_nothing_more() {
    let pool = Fleet::default();
    let (executor_tx, _executor_rx) = mpsc::unbounded_channel();
    let generation = pool.attach_executor(executor_tx);
    let residency = fleet_residency(
        ResidencyHolder::Service {
            service_id: "channel-imessage".into(),
        },
        None,
    );
    let detached = subscribed_events(&pool);
    let held = Arc::new(Mutex::new(Vec::new()));
    let captured = held.clone();
    let subscription = pool.subscribe_resident_process_events(move |event| {
        captured.lock().unwrap().push(event);
    });

    pool.handle_executor_message(
        COLOCATED_EXECUTOR_ID,
        generation,
        service_output(&residency),
    );
    assert_eq!(held.lock().unwrap().len(), 1);

    drop(subscription);
    assert_eq!(pool.resident_process_subscriber_count(), 1);
    pool.handle_executor_message(
        COLOCATED_EXECUTOR_ID,
        generation,
        service_output(&residency),
    );
    assert_eq!(
        held.lock().unwrap().len(),
        1,
        "an unregistered subscriber hears nothing more"
    );
    assert_eq!(
        detached.lock().unwrap().len(),
        2,
        "a detached subscriber stays registered"
    );
}

/// Dispatch does not hold the registry while subscribers run, so the owner
/// a subscriber releases can unregister it without deadlocking.
#[test]
fn a_subscriber_can_unregister_itself_while_it_is_dispatched_to() {
    let pool = Fleet::default();
    let (executor_tx, _executor_rx) = mpsc::unbounded_channel();
    let generation = pool.attach_executor(executor_tx);
    let residency = fleet_residency(
        ResidencyHolder::Service {
            service_id: "channel-imessage".into(),
        },
        None,
    );
    let slot = Arc::new(Mutex::new(None::<ResidentProcessSubscription>));
    let owner = slot.clone();
    let subscription = pool.subscribe_resident_process_events(move |_| {
        drop(owner.lock().unwrap().take());
    });
    *slot.lock().unwrap() = Some(subscription);

    pool.handle_executor_message(
        COLOCATED_EXECUTOR_ID,
        generation,
        service_output(&residency),
    );
    assert_eq!(pool.resident_process_subscriber_count(), 0);
}

fn resident_output_text(events: &[ResidentProcessEvent]) -> String {
    events
        .iter()
        .filter_map(|event| match &event.event {
            ResidentProcessEventKind::Output { data, .. } => {
                Some(String::from_utf8_lossy(data).into_owned())
            }
            _ => None,
        })
        .collect()
}

/// The split that a per-chunk pass cannot catch. Relayed output arrives in
/// frames whose boundaries fall wherever the executor's coalescer put them,
/// so the halves of one credential routinely land in two events.
#[test]
fn a_credential_split_across_two_resident_events_is_caught() {
    let _guard = register_resident_secret("resident-split");
    for at in 1..RESIDENT_SECRET.len() {
        let pool = Fleet::default();
        let (executor_tx, _executor_rx) = mpsc::unbounded_channel();
        let generation = pool.attach_executor(executor_tx);
        let received = subscribed_events(&pool);
        let residency = fleet_residency(
            ResidencyHolder::Service {
                service_id: "channel-imessage".into(),
            },
            None,
        );
        let output = |data: &[u8]| ResidentProcessEvent {
            holder: residency.holder.clone(),
            incarnation_id: residency.incarnation_id.clone(),
            cell_epoch: 7,
            process_key: "main".into(),
            process_generation: 2,
            event: ResidentProcessEventKind::Output {
                sequence: 1,
                stream: cairn_common::executor_protocol::ResidentProcessStream::Stdout,
                data: data.to_vec(),
            },
        };
        let full = format!("env: {RESIDENT_SECRET}\n");
        let (left, right) = full.split_at(at);
        for chunk in [left, right] {
            pool.handle_executor_message(
                COLOCATED_EXECUTOR_ID,
                generation,
                ExecutorMessage::ResidentProcessEvent {
                    event: output(chunk.as_bytes()),
                },
            );
        }
        // The exit is what drains the stream, and asserting the total only
        // after it is deliberate. How much a scrubber withholds mid-stream is
        // a schedule, not a guarantee, and this registry is the process-wide
        // one — any other test in this binary that materializes a callback
        // credential adds needles to it and shifts that schedule. Pinning the
        // schedule would make this test fail depending on what ran beside it;
        // pinning the total is the property it is named for.
        pool.handle_executor_message(
            COLOCATED_EXECUTOR_ID,
            generation,
            ExecutorMessage::ResidentProcessEvent {
                event: exited(&residency),
            },
        );
        let seen = resident_output_text(&received.lock().unwrap());
        assert!(
            !seen.contains(RESIDENT_SECRET),
            "leaked at split {at}: {seen}"
        );
        assert_eq!(seen, "env: [REDACTED]\n", "split {at}");
    }
}

/// The exit event for the fixture's resident process.
fn exited(residency: &CellResidency) -> ResidentProcessEvent {
    ResidentProcessEvent {
        holder: residency.holder.clone(),
        incarnation_id: residency.incarnation_id.clone(),
        cell_epoch: 7,
        process_key: "main".into(),
        process_generation: 2,
        event: ResidentProcessEventKind::State {
            status: cairn_common::executor_protocol::ResidentProcessStatus::Exited {
                finished_at_unix_ms: 42,
                exit_code: Some(0),
                restartable: false,
                executor_lost: false,
            },
        },
    }
}

/// The withheld tail has to reach the scrollback BEFORE the exit that seals
/// it. A terminal captures its output tail when it finalizes, so a tail
/// released after the exit event lands in a buffer nobody reads again.
#[test]
fn a_resident_exit_releases_the_withheld_tail_before_the_exit_itself() {
    let _guard = register_resident_secret("resident-exit");
    let pool = Fleet::default();
    let (executor_tx, _executor_rx) = mpsc::unbounded_channel();
    let generation = pool.attach_executor(executor_tx);
    let received = subscribed_events(&pool);
    let residency = fleet_residency(
        ResidencyHolder::Service {
            service_id: "channel-imessage".into(),
        },
        None,
    );
    let event = |kind| ResidentProcessEvent {
        holder: residency.holder.clone(),
        incarnation_id: residency.incarnation_id.clone(),
        cell_epoch: 7,
        process_key: "main".into(),
        process_generation: 2,
        event: kind,
    };
    // Ends on a genuine proper prefix of the registered credential, which is
    // what the scrubber must withhold: at this moment those bytes are
    // indistinguishable from the head of a value whose tail is in the next
    // chunk. Chosen deliberately rather than relying on ordinary text
    // happening to be withheld, so the test does not depend on what else
    // this binary registered into the process-wide registry.
    let head = &RESIDENT_SECRET[..8];
    pool.handle_executor_message(
        COLOCATED_EXECUTOR_ID,
        generation,
        ExecutorMessage::ResidentProcessEvent {
            event: event(ResidentProcessEventKind::Output {
                sequence: 1,
                stream: cairn_common::executor_protocol::ResidentProcessStream::Stdout,
                data: format!("done {head}").into_bytes(),
            }),
        },
    );
    assert_eq!(
        resident_output_text(&received.lock().unwrap()),
        "done ",
        "the unsettled tail must wait"
    );
    pool.handle_executor_message(
        COLOCATED_EXECUTOR_ID,
        generation,
        ExecutorMessage::ResidentProcessEvent {
            event: event(ResidentProcessEventKind::State {
                status: cairn_common::executor_protocol::ResidentProcessStatus::Exited {
                    finished_at_unix_ms: 42,
                    exit_code: Some(0),
                    restartable: false,
                    executor_lost: false,
                },
            }),
        },
    );
    let seen = received.lock().unwrap();
    assert_eq!(
        resident_output_text(&seen),
        format!("done {head}"),
        "the exit owes the stream its tail"
    );
    let last = seen.last().expect("an event");
    assert!(
        matches!(last.event, ResidentProcessEventKind::State { .. }),
        "the tail must arrive before the exit, not after it"
    );
}

/// The model-visible copy of a batch's output, and the diagnostics that
/// quote a failing command's output alongside it.
#[test]
fn a_terminal_outcome_is_scrubbed_before_its_waiter_sees_it() {
    let _guard = register_resident_secret("resident-outcome");
    let mut completed = CellOutcome::Completed {
        request_id: "r".into(),
        attempt_id: "a".into(),
        exit_code: Some(0),
        output: format!("CAIRN_MCP_SECRET={RESIDENT_SECRET}"),
        timed_out: false,
        metadata: CellExecutionMeta {
            commands_exited_at_unix_ms: None,
            warmth: None,
            load_context: None,
            executor_id: "executor".into(),
            executor_device_id: "device".into(),
            executor_connection_generation: 1,
            cell_id: "cell".into(),
            cell_epoch: 1,
            started_at_unix_ms: 1,
            finished_at_unix_ms: 2,
            duration_ms: None,
            peak_rss_bytes: None,
            peak_physical_footprint_bytes: None,
            peak_process: None,
            cpu: None,
            allocation: None,
            disk_delta_bytes: None,
            measurement_quality: None,
            environment_fingerprint: String::new(),
            verdict_platform: None,
            verdict_arch: None,
            verdict_environment_hash: None,
            sandbox: None,
            toolchain_fingerprint: None,
            containment: None,
            stop: None,
            cell_origin: None,
            test_phase: None,
            build_job_processes: None,
        },
        mutation_delta: None,
        sandbox_denials: Vec::new(),
        tracked_modifications: None,
        node_scratch: None,
    };
    scrub_cell_outcome(&mut completed);
    match &completed {
        CellOutcome::Completed { output, .. } => {
            assert_eq!(output, "CAIRN_MCP_SECRET=[REDACTED]");
        }
        other => panic!("unexpected outcome {other:?}"),
    }

    let mut unavailable = CellOutcome::Unavailable {
        reason: CellUnavailableReason::Spawn,
        diagnostic: format!("spawn failed: {RESIDENT_SECRET}"),
    };
    scrub_cell_outcome(&mut unavailable);
    match &unavailable {
        CellOutcome::Unavailable { diagnostic, .. } => {
            assert_eq!(diagnostic, "spawn failed: [REDACTED]");
        }
        other => panic!("unexpected outcome {other:?}"),
    }
}

/// The capability lives exactly as long as the context that authorizes it.
///
/// Every revocation path removes from `runner_contexts`, so this checks the
/// registration is released by the map entry going away rather than by any
/// one call site remembering to do it (CAIRN-3385).
#[test]
fn revoking_a_runner_context_unregisters_its_capability() {
    use crate::security::registry;

    let capability = "runner-context-capability-3824";
    let label = "request-3824:attempt-3824";
    // Matched by prefix: the id carries a process-local sequence so that two
    // live contexts can never spell the same one and silently replace each
    // other's registered forms.
    let registered = |present: bool| {
        let prefix = format!("batch-capability:{label}:");
        assert_eq!(
            registry()
                .metadata()
                .iter()
                .any(|entry| entry.id.as_str().starts_with(&prefix)),
            present
        );
    };
    let pool = Fleet::default();
    pool.runner_contexts.lock().unwrap().insert(
        capability.into(),
        RunnerContextEntry::new(
            capability,
            label,
            RunnerCallbackContext {
                request: None,
                run_context: None,
                check_status_board: None,
                run_batch_request_id: None,
                live_checkout: false,
                item_settlements: None,
                executor_binding: Some(RunnerContextExecutorBinding {
                    executor_id: "executor-3824".into(),
                    link: BatchLink::Bound(1),
                    request_id: "request-3824".into(),
                    attempt_id: "attempt-3824".into(),
                }),
                output: output_guard::RelayedOutput::shared(),
            },
        ),
    );
    registered(true);
    let mut sanitizer = crate::security::Sanitizer::exact();
    assert_eq!(sanitizer.text(capability), "[REDACTED]");

    pool.settle_runner_contexts_for_request("request-3824", "attempt-3824");
    registered(false);
}

/// An executor's snapshot of what it is running and its events about that
/// work travel as separate streams, so the event can land first. It has to
/// reach subscribers anyway: a process that lives a second cannot be made
/// to wait for the runner's cache to agree about it (CAIRN-3444).
#[test]
fn resident_events_survive_a_snapshot_that_has_not_caught_up() {
    let pool = Fleet::default();
    let (executor_tx, _executor_rx) = mpsc::unbounded_channel();
    let generation = pool.attach_executor(executor_tx);
    let received = Arc::new(Mutex::new(Vec::new()));
    let captured = received.clone();
    pool.subscribe_resident_process_events(move |event| {
        captured.lock().unwrap().push(event);
    })
    .detach();
    let residency = fleet_residency(
        ResidencyHolder::Service {
            service_id: "channel-imessage".into(),
        },
        None,
    );
    let event = |cell_epoch: u64, process_generation: u64| ResidentProcessEvent {
        holder: residency.holder.clone(),
        incarnation_id: residency.incarnation_id.clone(),
        cell_epoch,
        process_key: "main".into(),
        process_generation,
        event: ResidentProcessEventKind::Output {
            sequence: 1,
            stream: cairn_common::executor_protocol::ResidentProcessStream::Stdout,
            data: b"guid".to_vec(),
        },
    };

    // Nothing is known about this cell yet: the residency was acquired and
    // the process started since the last beat.
    pool.handle_executor_message(
        COLOCATED_EXECUTOR_ID,
        generation,
        ExecutorMessage::ResidentProcessEvent { event: event(7, 2) },
    );
    assert_eq!(received.lock().unwrap().len(), 1);

    // A snapshot that agrees changes nothing, and one that knows a later
    // generation at the same key refuses the process before it.
    pool.set_executor_snapshot(
        COLOCATED_EXECUTOR_ID,
        generation,
        FleetSnapshot {
            cells: vec![resident_process_cell(
                residency.clone(),
                7,
                2,
                cairn_common::executor_protocol::ResidentProcessStatus::Running {
                    started_at_unix_ms: 42,
                    process_group_id: None,
                    ownership: None,
                },
            )],
            ..Default::default()
        },
        ExecutorSubstrateReport::default(),
    );
    pool.handle_executor_message(
        COLOCATED_EXECUTOR_ID,
        generation,
        ExecutorMessage::ResidentProcessEvent { event: event(7, 2) },
    );
    assert_eq!(received.lock().unwrap().len(), 2);
    pool.handle_executor_message(
        COLOCATED_EXECUTOR_ID,
        generation,
        ExecutorMessage::ResidentProcessEvent { event: event(7, 1) },
    );
    assert_eq!(
        received.lock().unwrap().len(),
        2,
        "a generation the runner has already seen superseded is stale"
    );
    pool.handle_executor_message(
        COLOCATED_EXECUTOR_ID,
        generation,
        ExecutorMessage::ResidentProcessEvent { event: event(6, 9) },
    );
    assert_eq!(
        received.lock().unwrap().len(),
        2,
        "an epoch the cell has moved past is stale whatever its generation"
    );

    // A link that has since bounced speaks for nobody.
    pool.handle_executor_message(
        COLOCATED_EXECUTOR_ID,
        generation + 1,
        ExecutorMessage::ResidentProcessEvent { event: event(7, 2) },
    );
    assert_eq!(received.lock().unwrap().len(), 2);
}

fn darwin_remote_config() -> RemoteExecutorConfig {
    RemoteExecutorConfig {
        host: "bglab-mac.local".into(),
        ssh_user: "dev".into(),
        platform: RemotePlatform::DarwinArm64,
        binary_path: "/Users/dev/.local/bin/cairn-executor".into(),
        cairn_home: "/Users/dev/.cairn-executor".into(),
        executor_id: "bglab-mac".into(),
        device_id: "bglab-mac-device".into(),
        display_name: "bglab-mac".into(),
        project_ids: vec![],
        bootstrapped_at_unix_ms: None,
        legacy_retired_at_unix_ms: None,
        legacy_tunnel_port: None,
        extra_ssh_args: vec![],
    }
}

#[test]
fn darwin_arm64_reports_macos_identity_and_the_apple_silicon_target() {
    assert_eq!(RemotePlatform::DarwinArm64.os(), "macos");
    assert_eq!(RemotePlatform::DarwinArm64.arch(), "aarch64");
    assert_eq!(RemotePlatform::DarwinArm64.target(), "aarch64-apple-darwin");
    // `arch()` was a constant "x86_64" before Darwin existed; the other two
    // platforms must be unaffected by its promotion to a match.
    assert_eq!(RemotePlatform::LinuxX86_64.arch(), "x86_64");
    assert_eq!(RemotePlatform::WindowsX86_64.arch(), "x86_64");
}

#[test]
fn linux_arm64_reports_linux_identity_and_the_gnu_arm_target() {
    assert_eq!(RemotePlatform::LinuxArm64.os(), "linux");
    assert_eq!(RemotePlatform::LinuxArm64.arch(), "aarch64");
    assert_eq!(
        RemotePlatform::LinuxArm64.target(),
        "aarch64-unknown-linux-gnu"
    );
}

/// Every enrolled machine is described twice: by this enum before it
/// attaches, and by its own advertisement after. `std::env::consts::ARCH` is
/// what the executor advertises, so a variant whose `arch()` disagreed with
/// it would make one machine appear to change architecture on attach.
#[test]
fn arch_agrees_with_what_an_attached_executor_advertises() {
    for platform in [
        RemotePlatform::LinuxX86_64,
        RemotePlatform::LinuxArm64,
        RemotePlatform::WindowsX86_64,
        RemotePlatform::DarwinArm64,
    ] {
        // Rust spells these the same way on every host it supports, so the
        // triple each variant names carries the arch it must report.
        assert!(
            platform.target().starts_with(platform.arch()),
            "{platform:?} reports arch {} but targets {}",
            platform.arch(),
            platform.target()
        );
    }

    // And concretely, against the machine running this test: whichever
    // variant describes this host must report what this host reports.
    let host = if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
        Some(RemotePlatform::DarwinArm64)
    } else if cfg!(all(target_os = "linux", target_arch = "x86_64")) {
        Some(RemotePlatform::LinuxX86_64)
    } else if cfg!(all(target_os = "linux", target_arch = "aarch64")) {
        Some(RemotePlatform::LinuxArm64)
    } else if cfg!(all(target_os = "windows", target_arch = "x86_64")) {
        Some(RemotePlatform::WindowsX86_64)
    } else {
        None
    };
    if let Some(host) = host {
        assert_eq!(std::env::consts::ARCH, host.arch());
        assert_eq!(std::env::consts::OS, host.os());
    }
}

#[test]
fn linux_arm64_paths_validate_by_the_posix_absolute_rule() {
    let config = RemoteExecutorConfig {
        platform: RemotePlatform::LinuxArm64,
        binary_path: "/home/dev/.local/bin/cairn-executor".into(),
        cairn_home: "/home/dev/.cairn-executor".into(),
        ..darwin_remote_config()
    };
    config.validate().unwrap();

    let windows_shaped = RemoteExecutorConfig {
        binary_path: r"C:\cairn\cairn-executor".into(),
        ..config.clone()
    };
    assert!(windows_shaped.validate().is_err());

    // The wire spelling settings files carry, and the one
    // `packages/ui/src/api/fleet.ts` mirrors.
    let encoded = serde_json::to_value(&config).unwrap();
    assert_eq!(encoded["platform"], serde_json::json!("linux-arm64"));
    assert_eq!(
        serde_json::from_value::<RemoteExecutorConfig>(encoded).unwrap(),
        config
    );
}

#[test]
fn darwin_paths_validate_by_the_posix_absolute_rule() {
    darwin_remote_config().validate().unwrap();
    let relative = RemoteExecutorConfig {
        binary_path: ".local/bin/cairn-executor".into(),
        ..darwin_remote_config()
    };
    assert!(relative
        .validate()
        .unwrap_err()
        .contains("binaryPath must be an absolute path"));
    let windows_shaped = RemoteExecutorConfig {
        binary_path: r"C:\cairn\cairn-executor".into(),
        ..darwin_remote_config()
    };
    assert!(windows_shaped.validate().is_err());
}

#[test]
fn platform_round_trips_as_kebab_case_and_defaults_to_linux_when_absent() {
    let encoded = serde_json::to_value(darwin_remote_config()).unwrap();
    assert_eq!(encoded["platform"], serde_json::json!("darwin-arm64"));
    assert_eq!(
        serde_json::from_value::<RemoteExecutorConfig>(encoded).unwrap(),
        darwin_remote_config()
    );

    // Settings files written before Darwin existed carry no `platform` key.
    let mut legacy = serde_json::to_value(darwin_remote_config()).unwrap();
    legacy.as_object_mut().unwrap().remove("platform");
    assert_eq!(
        serde_json::from_value::<RemoteExecutorConfig>(legacy)
            .unwrap()
            .platform,
        RemotePlatform::LinuxX86_64
    );
}

// Bound the entire scenario, including fixture setup and all join branches.
// Keeping futures in the join means a timeout drops every acquisition too.
async fn residency_queue_scenario(future: impl std::future::Future<Output = ()>) {
    tokio::time::timeout(Duration::from_secs(20), future)
        .await
        .expect("residency queue scenario exceeded its bounded wait");
}

async fn residency_queue_frame(
    receiver: &mut mpsc::UnboundedReceiver<ExecutorMessage>,
) -> ExecutorMessage {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match receiver.recv().await {
                Some(ExecutorMessage::DrainModeRequest { .. }) => continue,
                Some(frame) => return frame,
                None => panic!("executor channel closed before expected residency frame"),
            }
        }
    })
    .await
    .expect("runner did not send the expected residency frame")
}

fn residency_queue_attach(
    pool: &Fleet,
    id: &str,
) -> (u64, mpsc::UnboundedReceiver<ExecutorMessage>) {
    residency_queue_attach_on(pool, id, "linux")
}

fn residency_queue_attach_on(
    pool: &Fleet,
    id: &str,
    os: &str,
) -> (u64, mpsc::UnboundedReceiver<ExecutorMessage>) {
    let (sender, receiver) = mpsc::unbounded_channel();
    let (_, advertised) = fleet_entry(id, os, 0, &[]);
    let generation = pool
        .attach_advertised_executor(
            Arc::unwrap_or_clone(advertised.advertisement),
            Default::default(),
            sender,
            id == COLOCATED_EXECUTOR_ID,
            None,
            None,
        )
        .unwrap();
    measured_at(
        pool.lock_connections().get_mut(id).unwrap(),
        0.0,
        32 * 1024 * 1024 * 1024,
        512 * 1024 * 1024 * 1024,
        unix_time_ms(),
    );
    pool.announce_room_for_test(id, generation);
    (generation, receiver)
}

fn residency_queue_request(job: &str) -> ResidencyAcquireRequest {
    let mut request = fleet_residency_request(ResidencyHolder::Job { job_id: job.into() });
    request.repository = RepositoryLocator::ScratchOnly {
        owner_id: "p".into(),
    };
    request.waiting_since_unix_ms = unix_time_ms();
    request.wait_horizon_unix_ms = unix_time_ms() + 10_000;
    request.executor = Some(ExecutorSelector {
        name: Some("remote".into()),
        ..Default::default()
    });
    request
}

fn residency_queue_reply(
    pool: &Fleet,
    executor: &str,
    generation: u64,
    correlation_id: String,
    request: &ResidencyAcquireRequest,
) {
    let mut cell = materialization_read_cell("queue-cell", "queue-job");
    cell.executor_id = executor.into();
    let residency = cell.residency.as_mut().unwrap();
    residency.holder = request.holder.clone();
    residency.repository = request.repository.clone();
    pool.handle_executor_message(
        executor,
        generation,
        ExecutorMessage::ResidencyResponse {
            correlation_id,
            result: ResidencyResult::State { cell },
        },
    );
}

async fn residency_queue_offer(receiver: &mut mpsc::UnboundedReceiver<ExecutorMessage>) -> String {
    match residency_queue_frame(receiver).await {
        ExecutorMessage::Offer { entry_id, summary } => {
            assert_eq!(summary.admission_kind, CellAdmissionKind::Residency);
            assert_eq!(summary.attempt_id, RESIDENCY_ACQUIRE_ATTEMPT_ID);
            entry_id
        }
        other => panic!("fresh acquisition must offer before granting: {other:?}"),
    }
}

async fn residency_queue_grant(
    receiver: &mut mpsc::UnboundedReceiver<ExecutorMessage>,
    entry: Option<&str>,
) -> (String, ResidencyAcquireRequest) {
    match residency_queue_frame(receiver).await {
        ExecutorMessage::ResidencyRequest {
            correlation_id,
            granted_entry_id,
            config: ResidencyRuntimeConfig::Install(_),
            operation: ResidencyOperation::Acquire { request },
        } => {
            assert_eq!(granted_entry_id.as_deref(), entry);
            (correlation_id, request)
        }
        other => panic!("expected acquisition with exact grant/config, got {other:?}"),
    }
}

/// Grant `job`'s fresh acquisition on `remote` and return the cell the
/// grant answered with. No snapshot carries it.
async fn residency_grant_on_remote(
    orch: &Orchestrator,
    remote: &mut mpsc::UnboundedReceiver<ExecutorMessage>,
    generation: u64,
    job: &str,
) -> PersistentCellState {
    let request = residency_queue_request(job);
    let (result, ()) = tokio::join!(
        orch.fleet.operate_residency(
            orch,
            ResidencyOperation::Acquire {
                request: request.clone()
            }
        ),
        async {
            let entry = residency_queue_offer(remote).await;
            orch.fleet.handle_executor_message(
                "remote",
                generation,
                ExecutorMessage::Claim {
                    entry_id: entry.clone(),
                },
            );
            let (correlation, granted) = residency_queue_grant(remote, Some(&entry)).await;
            residency_queue_reply(&orch.fleet, "remote", generation, correlation, &granted);
        }
    );
    let ResidencyResult::State { cell } = result else {
        panic!("the acquisition is granted: {result:?}");
    };
    cell
}

fn residency_fence_of(cell: &PersistentCellState) -> ResidencyFence {
    let residency = cell.residency.as_ref().unwrap();
    ResidencyFence {
        holder: residency.holder.clone(),
        incarnation_id: residency.incarnation_id.clone(),
        cell_epoch: cell.cell_epoch,
    }
}

/// Stop a process in `cell` and answer for `executor` on `generation`,
/// asserting the operation went out on that link with the cell's fence.
async fn residency_stop_routed_to(
    orch: &Orchestrator,
    receiver: &mut mpsc::UnboundedReceiver<ExecutorMessage>,
    executor: &str,
    generation: u64,
    cell: &PersistentCellState,
) -> ResidencyResult {
    let fence = residency_fence_of(cell);
    let (result, ()) = tokio::join!(
        orch.fleet.operate_residency(
            orch,
            ResidencyOperation::StopProcess {
                fence: fence.clone(),
                process_key: "svc".into(),
            }
        ),
        async {
            let correlation_id = match residency_queue_frame(receiver).await {
                ExecutorMessage::ResidencyRequest {
                    correlation_id,
                    operation: ResidencyOperation::StopProcess { fence: sent, .. },
                    ..
                } => {
                    assert_eq!(sent, fence);
                    correlation_id
                }
                other => panic!("the stop goes out on {executor}'s link: {other:?}"),
            };
            orch.fleet.handle_executor_message(
                executor,
                generation,
                ExecutorMessage::ResidencyResponse {
                    correlation_id,
                    result: ResidencyResult::State { cell: cell.clone() },
                },
            );
        }
    );
    result
}

/// A snapshot taken before the grant can reach the runner after the grant's
/// answer. It does not carry the cell, and rewriting the executor's routes
/// from it must not erase the route the grant just recorded.
#[tokio::test]
async fn a_snapshot_older_than_the_grant_keeps_the_grants_route() {
    residency_queue_scenario(async {
        let temp = tempfile::tempdir().unwrap();
        let orch = test_orchestrator(temp.path()).await;
        let (generation, mut remote) = residency_queue_attach(&orch.fleet, "remote");
        let cell = residency_grant_on_remote(&orch, &mut remote, generation, "stale").await;
        let key = (
            "remote".to_string(),
            cell.residency.as_ref().unwrap().holder.storage_key(),
        );
        orch.fleet.handle_executor_message(
            "remote",
            generation,
            ExecutorMessage::SnapshotUpdated {
                snapshot: FleetSnapshot::default(),
                health: ExecutorSubstrateReport::default(),
            },
        );
        assert!(orch
            .fleet
            .residency_routes
            .lock()
            .unwrap()
            .contains_key(&key));
        assert!(orch.fleet.granted_cells.lock().unwrap().contains_key(&key));
        let result =
            residency_stop_routed_to(&orch, &mut remote, "remote", generation, &cell).await;
        assert!(
            matches!(result, ResidencyResult::State { .. }),
            "{result:?}"
        );
    })
    .await;
}

/// After a runner restart no grant is held, and a route recorded only from
/// an executor's report routes exactly as it always has.
#[tokio::test]
async fn a_route_recorded_only_by_a_report_routes_to_its_executor() {
    residency_queue_scenario(async {
        let temp = tempfile::tempdir().unwrap();
        let orch = test_orchestrator(temp.path()).await;
        let (generation, mut remote) = residency_queue_attach(&orch.fleet, "remote");
        let mut cell = materialization_read_cell("reported-cell", "reported");
        cell.executor_id = "remote".into();
        orch.fleet.handle_executor_message(
            "remote",
            generation,
            ExecutorMessage::SnapshotUpdated {
                snapshot: FleetSnapshot {
                    cells: vec![cell.clone()],
                    ..FleetSnapshot::default()
                },
                health: ExecutorSubstrateReport::default(),
            },
        );
        assert!(orch.fleet.granted_cells.lock().unwrap().is_empty());
        let result =
            residency_stop_routed_to(&orch, &mut remote, "remote", generation, &cell).await;
        assert!(
            matches!(result, ResidencyResult::State { .. }),
            "{result:?}"
        );
    })
    .await;
}

fn completed_for(request: &CellRequest, executor_id: &str, generation: u64) -> CellOutcome {
    CellOutcome::Completed {
        request_id: request.request_id.clone(),
        attempt_id: request.attempt_id.clone(),
        exit_code: Some(0),
        output: format!("ran {}", request.request_id),
        timed_out: false,
        metadata: CellExecutionMeta {
            commands_exited_at_unix_ms: None,
            warmth: None,
            load_context: None,
            executor_id: executor_id.into(),
            executor_device_id: "device".into(),
            executor_connection_generation: generation,
            cell_id: "cell".into(),
            cell_epoch: 1,
            started_at_unix_ms: 1,
            finished_at_unix_ms: 2,
            duration_ms: None,
            peak_rss_bytes: None,
            peak_physical_footprint_bytes: None,
            peak_process: None,
            cpu: None,
            allocation: None,
            disk_delta_bytes: None,
            measurement_quality: None,
            environment_fingerprint: String::new(),
            verdict_platform: None,
            verdict_arch: None,
            verdict_environment_hash: None,
            sandbox: None,
            toolchain_fingerprint: None,
            containment: None,
            stop: None,
            cell_origin: None,
            test_phase: None,
            build_job_processes: None,
        },
        mutation_delta: None,
        sandbox_denials: Vec::new(),
        tracked_modifications: None,
        node_scratch: None,
    }
}

/// The next frame that is about work, skipping the runner restating its
/// drain posture and its answer to a heartbeat.
async fn next_work_frame(
    receiver: &mut mpsc::UnboundedReceiver<ExecutorMessage>,
) -> Option<ExecutorMessage> {
    loop {
        match receiver.recv().await {
            Some(
                ExecutorMessage::DrainModeRequest { .. } | ExecutorMessage::WaitingRequests { .. },
            ) => continue,
            other => return other,
        }
    }
}

/// A shared execution belongs to everyone who joined it, not to the caller
/// that happened to open it.
///
/// A turn-end review wave whose hook fired off any runtime runs on a
/// current-thread runtime of its own, so a leader it opens is spawned from
/// there. Cancelling the wave returns from that runtime's `block_on` and
/// drops the runtime, which ends every task still on it. A leader that ran
/// there died mid-execution with its completion guard armed, and every
/// consumer that had joined it was told the leader vanished while the
/// machine was still doing the work.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shared_execution_outlives_the_runtime_of_the_wave_that_opened_it() {
    use cairn_common::executor_protocol::ProcessBatchItemOutcome;
    let temp = tempfile::tempdir().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    let (sender, mut executor) = mpsc::unbounded_channel();
    let generation = orch.fleet.attach_announced_executor(sender);
    let check = |request_id: &str| {
        let mut request = provenance_request();
        request.request_id = request_id.into();
        request.repository = RepositoryLocator::ScratchOnly {
            owner_id: request.project_id.clone(),
        };
        request.wait_horizon_unix_ms = unix_time_ms() + 60_000;
        let item = PureVerdictBatchItem {
            result_identity: result_identity(),
            process: cache_batch_item(Vec::new()),
        };
        (request, item)
    };

    let (cancel_wave, wave_cancelled) = oneshot::channel::<()>();
    let wave_orch = orch.clone();
    let (request, item) = check("wave");
    let wave = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            tokio::select! {
                _ = wave_orch
                    .fleet
                    .submit_pure_verdict_check(&wave_orch, request, item, None, None, None) => {
                    panic!("the wave is cancelled before its check settles")
                }
                _ = wave_cancelled => {}
            }
        });
        // The runtime drops here, as the wave's own does when it returns.
    });
    let Some(ExecutorMessage::Offer { entry_id, .. }) = next_work_frame(&mut executor).await else {
        panic!("the wave's check is offered");
    };

    let follower_orch = orch.clone();
    let (request, item) = check("follower");
    let follower = tokio::spawn(async move {
        follower_orch
            .fleet
            .submit_pure_verdict_check(&follower_orch, request, item, None, None, None)
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !orch
            .fleet
            .in_flight
            .lock()
            .unwrap()
            .by_key
            .values()
            .any(|execution| execution.subscribers.len() == 2)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the follower joins the wave's execution");

    cancel_wave.send(()).unwrap();
    tokio::task::spawn_blocking(move || wave.join())
        .await
        .unwrap()
        .expect("the wave's thread ends cleanly");

    orch.fleet.handle_executor_message(
        COLOCATED_EXECUTOR_ID,
        generation,
        ExecutorMessage::Claim { entry_id },
    );
    let granted = tokio::time::timeout(Duration::from_secs(10), next_work_frame(&mut executor))
        .await
        .ok()
        .flatten();
    let Some(ExecutorMessage::Granted { request, .. }) = granted else {
        panic!("the shared check is still granted after the wave is gone; got {granted:?}");
    };
    let mut outcome = completed_for(&request, COLOCATED_EXECUTOR_ID, generation);
    if let CellOutcome::Completed { output, .. } = &mut outcome {
        *output = serde_json::to_string(&[ProcessBatchItemOutcome {
            header: "item".into(),
            body: "ok".into(),
            succeeded: true,
            suspended: false,
            images: Vec::new(),
            exit_code: Some(0),
            timed_out: false,
            started_at_unix_ms: 1,
            finished_at_unix_ms: 2,
            duration_ms: 1,
            peak_rss_bytes: None,
            peak_process: None,
            cpu: None,
            granted_units: None,
            disk_delta_bytes: None,
            sandbox_denials: Vec::new(),
            tracked_modifications: None,
            environment_fingerprint: String::new(),
            verdict_platform: None,
            verdict_arch: None,
            toolchain_fingerprint: None,
            verdict_environment_hash: None,
            stop: None,
            build_jobs: None,
            unrecorded_cache_checkpoint: None,
            build_job_processes: None,
        }])
        .unwrap();
    }
    orch.fleet.handle_executor_message(
        COLOCATED_EXECUTOR_ID,
        generation,
        ExecutorMessage::Result {
            request_id: request.request_id.clone(),
            attempt_id: request.attempt_id.clone(),
            outcome,
        },
    );

    let delivered = tokio::time::timeout(Duration::from_secs(10), follower)
        .await
        .expect("the follower is answered")
        .unwrap();
    assert!(
        matches!(
            &delivered,
            Ok(CoalescedCellOutcome {
                outcome: CellOutcome::Completed {
                    exit_code: Some(0),
                    ..
                },
                ..
            })
        ),
        "the follower receives the machine's answer, not a vanished leader: {:?}",
        delivered.as_ref().map(|delivered| &delivered.outcome)
    );
}

/// An early settlement and its batch's result are compared on what was
/// decided, never on what was measured: an early report reads the batch's
/// test phase as far as it had got.
#[test]
fn an_early_settlement_is_checked_against_its_batch_on_the_verdict_alone() {
    let request = provenance_request();
    let early = completed_for(&request, COLOCATED_EXECUTOR_ID, 1);
    let mut measured_apart = early.clone();
    if let CellOutcome::Completed { metadata, .. } = &mut measured_apart {
        metadata.duration_ms = Some(999);
        metadata.peak_rss_bytes = Some(1 << 30);
    }
    assert!(verdict_disagreement(&early, &measured_apart).is_empty());
    let mut red = early.clone();
    if let CellOutcome::Completed {
        exit_code, output, ..
    } = &mut red
    {
        *exit_code = Some(1);
        *output = "something else".into();
    }
    assert_eq!(verdict_disagreement(&early, &red), ["exit code", "output"]);
    let cancelled = CellOutcome::Cancelled {
        request_id: request.request_id.clone(),
        attempt_id: request.attempt_id.clone(),
    };
    assert_eq!(verdict_disagreement(&early, &cancelled), ["outcome"]);
}

/// An agent's batch carries no command identities, and its items' kills
/// live only in their own outcomes. Through the real completion path each
/// item teaches its own command class: the two cargo items killed below
/// the fit line count two crowded kills because their starting load is
/// unknown, and the vitest item's process is learned under vitest, not
/// cargo. Those kills teach the victim's size without ending the fit.
///
/// The runner mints an identity for each cargo item and none for vitest,
/// which runs first, so each cargo identity must learn its own item's kill
/// rather than the outcome one position before it.
#[tokio::test]
async fn an_identity_free_batch_teaches_each_items_suite_and_counts_its_kills() {
    use cairn_common::executor_protocol::ProcessBatchItemOutcome;
    const GIB: u64 = 1 << 30;
    let temp = tempfile::tempdir().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    let (sender, mut executor) = mpsc::unbounded_channel();
    let generation = orch.fleet.attach_announced_executor(sender);
    let commands = [
        "bun run test:frontend",
        "cargo test -p cairn-core",
        "cargo test -p cairn-db",
    ];
    let items = commands
        .iter()
        .enumerate()
        .map(|(index, command)| ProcessBatchItem {
            header: command.to_string(),
            stream_id: format!("stream:{index}"),
            args: vec![command.to_string()],
            ..cache_batch_item(Vec::new())
        })
        .collect::<Vec<_>>();
    let fleet = orch.fleet.clone();
    let machine = tokio::spawn(async move {
        let Some(ExecutorMessage::Offer { entry_id, .. }) = next_work_frame(&mut executor).await
        else {
            panic!("the batch is offered");
        };
        fleet.handle_executor_message(
            COLOCATED_EXECUTOR_ID,
            generation,
            ExecutorMessage::Claim { entry_id },
        );
        let Some(ExecutorMessage::Granted { request, .. }) = next_work_frame(&mut executor).await
        else {
            panic!("granted");
        };
        let item = |header: &str,
                    peak: u64,
                    stop: Option<cairn_common::executor_protocol::CommandStop>| {
            ProcessBatchItemOutcome {
                header: header.into(),
                body: "output".into(),
                succeeded: stop.is_none(),
                suspended: false,
                images: Vec::new(),
                exit_code: Some(if stop.is_none() { 0 } else { 101 }),
                timed_out: false,
                started_at_unix_ms: 1,
                finished_at_unix_ms: 2,
                duration_ms: 1,
                peak_rss_bytes: Some(peak),
                peak_process: Some(cairn_common::executor_protocol::PeakProcess {
                    bytes: peak,
                    name: None,
                }),
                cpu: None,
                granted_units: None,
                disk_delta_bytes: None,
                sandbox_denials: Vec::new(),
                tracked_modifications: None,
                environment_fingerprint: String::new(),
                verdict_platform: None,
                verdict_arch: None,
                toolchain_fingerprint: None,
                verdict_environment_hash: None,
                stop,
                build_jobs: None,
                unrecorded_cache_checkpoint: None,
                build_job_processes: None,
            }
        };
        let killed = |pid| {
            Some(cairn_common::executor_protocol::CommandStop::OomKilled {
                pid,
                rss_bytes: Some(3 * GIB),
                machine_total_bytes: Some(13_000_000_000),
                process_name: Some("rustc".into()),
                bound_bytes: None,
            })
        };
        let outcomes = vec![
            item("vitest", GIB, None),
            item("cargo-core", 2 * GIB, killed(7)),
            item("cargo-db", 2 * GIB, killed(8)),
        ];
        let mut outcome = completed_for(&request, COLOCATED_EXECUTOR_ID, generation);
        if let CellOutcome::Completed { output, .. } = &mut outcome {
            *output = serde_json::to_string(&outcomes).unwrap();
        }
        fleet.handle_executor_message(
            COLOCATED_EXECUTOR_ID,
            generation,
            ExecutorMessage::Result {
                request_id: request.request_id.clone(),
                attempt_id: request.attempt_id.clone(),
                outcome,
            },
        );
    });
    let mut request = provenance_request();
    request.repository = RepositoryLocator::ScratchOnly {
        owner_id: request.project_id.clone(),
    };
    request.command_resource_identity = None;
    let project_id = request.project_id.clone();
    let batch = ProcessBatch {
        files: Vec::new(),
        runtime_packages: Vec::new(),
        sequential: true,
        stop_on_error: false,
        sandbox_mode: ProcessSandboxMode::Unconfined,
        timeout_bases: Vec::new(),
        items,
        runner_context_id: None,
        execution_residency: None,
        containment: None,
        node_scratch: None,
    };
    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        orch.fleet.submit_execution(&orch, request, Some(batch)),
    )
    .await
    .expect("the batch settles");
    machine.await.unwrap();
    assert!(
        matches!(outcome, CellOutcome::Completed { .. }),
        "{outcome:?}"
    );
    let db = orch.db.local.clone();
    let rows = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT suite_key, upper_process_peak_bytes, oom_kill_count, oom_crowded_kill_count FROM suite_memory_evidence ORDER BY suite_key",
                        (),
                    )
                    .await?;
                let mut found = Vec::new();
                while let Some(row) = rows.next().await? {
                    found.push((
                        row.get::<String>(0)?,
                        row.get::<i64>(1)? as u64,
                        row.get::<i64>(2)?,
                        row.get::<i64>(3)?,
                    ));
                }
                Ok(found)
            })
        })
        .await
        .unwrap();
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert!(rows[0].0.ends_with("/class:cargoTest"), "{rows:?}");
    assert_eq!(
        (rows[0].1, rows[0].2, rows[0].3),
        (3 * GIB, 0, 2),
        "two unknown-load kills teach the victim peak, never solo kills"
    );
    assert!(rows[1].0.ends_with("/class:vitest"), "{rows:?}");
    assert_eq!((rows[1].1, rows[1].2, rows[1].3), (GIB, 0, 0));
    let context = {
        let connections = orch.fleet.lock_connections();
        ReservationPlan::profile_context(&selected_executor(
            connections.get(COLOCATED_EXECUTOR_ID).unwrap(),
        ))
    };
    let cargo_test = cairn_common::executor_protocol::CellCommandClass::CargoTest;
    for package in ["cairn-core", "cairn-db"] {
        // A later run of the same command under another filter is the same
        // identity, and resolves to what this one measured of itself.
        let identity = resource_profiles::ad_hoc_identity(
            &project_id,
            &format!("cargo test -p {package} -- some::filter"),
            cargo_test,
        )
        .unwrap();
        let own = resource_profiles::resolve_process_demand(
            db.clone(),
            Some(&identity),
            Some(&rows[0].0),
            cargo_test,
            &context,
        )
        .await
        .demand
        .unwrap_or_else(|| panic!("{package} learned its own kill"));
        assert_eq!(
            (own.peak_bytes, own.suite.as_deref(), own.basis.clone()),
            (
                3 * GIB,
                None,
                cairn_common::executor_protocol::ProcessDemandBasis::ThisPlatform
            ),
            "{package} speaks for itself, not for the item before it"
        );
    }
    let evidence =
        resource_profiles::resolve_process_demand(db, None, Some(&rows[0].0), cargo_test, &context)
            .await;
    assert_eq!(
        judge_process_evidence(
            &evidence,
            13_000_000_000,
            Some(GIB),
            Some(11_042_782_208),
            None,
            &context.executor_class,
            true,
        ),
        None,
        "unknown-load kills below capacity never refuse a Linux machine"
    );
    let demand = evidence.demand.expect("the killed rustc is learned");
    assert_eq!(demand.peak_bytes, 3 * GIB);
    assert_eq!(demand.name.as_deref(), Some("rustc"));
    let kills = demand.kills_here.expect("the machine's kills are retained");
    assert_eq!(kills.alone, 0);
    assert_eq!(kills.crowded.unwrap().count, 2);
}

fn held_by(pid: u32) -> cairn_common::executor_protocol::ColocatedGenerationHold {
    cairn_common::executor_protocol::ColocatedGenerationHold {
        holder_pid: Some(pid),
        holder_acquired_at_unix_ms: Some(unix_time_ms() - 21_419_135),
        observed_since_unix_ms: unix_time_ms(),
        reason: cairn_common::executor_protocol::GenerationHoldReason::Unidentified,
    }
}

/// Only the launch the runner last issued a nonce to is its executor; an
/// empty nonce, which every executor that predates the binding sends, is
/// never admitted.
#[test]
fn only_the_last_issued_supervision_nonce_is_admitted() {
    let fleet = Fleet::default();
    assert!(!fleet.admits_colocated_supervision("first"));
    fleet.expect_colocated_supervision("first".into());
    assert!(fleet.admits_colocated_supervision("first"));
    fleet.expect_colocated_supervision("second".into());
    assert!(!fleet.admits_colocated_supervision("first"));
    assert!(fleet.admits_colocated_supervision("second"));
    assert!(!fleet.admits_colocated_supervision(""));
}

/// A failure streak is history, so it survives the declaration every
/// attempt replaces and ends only at readiness. The last handoff outlives
/// readiness, so an escalation stays visible after it worked.
#[test]
fn the_supervision_streak_survives_redeclaration_and_resets_on_readiness() {
    use cairn_common::executor_protocol::{
        ExecutorRetirementSummary, RetirementOrigin, RetirementOutcome, RetirementPhase,
    };
    let fleet = Fleet::default();
    fleet.declare_colocated_substrate_failure("first failure".into());
    let since = fleet.colocated_supervision().failing_since_unix_ms.unwrap();
    fleet.declare_colocated_substrate(ExecutorSubstrateState::SupervisorRespawning);
    fleet.declare_colocated_substrate_failure("second failure".into());
    let supervision = fleet.colocated_supervision();
    assert_eq!(supervision.failing_since_unix_ms, Some(since));
    assert_eq!(supervision.failed_attempts, 2);
    assert_eq!(supervision.last_cause.as_deref(), Some("second failure"));

    let retirement = ExecutorRetirementSummary {
        pid: 12184,
        origin: RetirementOrigin::PriorRunner,
        escalated_to: Some(RetirementPhase::Force),
        graceful_ms: 7_000,
        force_ms: Some(40),
        total_ms: 7_040,
        outcome: RetirementOutcome::Retired,
        completed_at_unix_ms: unix_time_ms(),
        workload: Default::default(),
    };
    fleet.record_colocated_retirement(retirement.clone());
    fleet.declare_colocated_generation_held(held_by(12184));
    fleet.clear_colocated_substrate();
    let supervision = fleet.colocated_supervision();
    assert_eq!(supervision.failing_since_unix_ms, None);
    assert_eq!(supervision.failed_attempts, 0);
    assert_eq!(supervision.generation_hold, None);
    assert_eq!(supervision.last_retirement, Some(retirement));
}

/// The same holder keeps the moment it was first found, so the view reads
/// how long it has been in the way, and the substrate names it.
#[test]
fn a_generation_hold_names_its_holder_and_keeps_its_first_sighting() {
    let fleet = Fleet::default();
    let mut first = held_by(12184);
    first.observed_since_unix_ms = 1_000;
    fleet.declare_colocated_generation_held(first);
    fleet.declare_colocated_generation_held(held_by(12184));
    assert_eq!(
        fleet
            .colocated_supervision()
            .generation_hold
            .map(|hold| hold.observed_since_unix_ms),
        Some(1_000)
    );
    let evidence = fleet.colocated_substrate().unwrap();
    assert_eq!(evidence.state, ExecutorSubstrateState::GenerationHeld);
    let diagnostic = evidence.diagnostic.unwrap();
    assert!(diagnostic.contains("pid 12184"), "{diagnostic}");
    assert!(diagnostic.contains("recorded 5h56m ago"), "{diagnostic}");
    assert!(diagnostic.contains("will not signal"), "{diagnostic}");
    // Not a recovery the caller can wait on.
    assert_eq!(
        fleet.link_restoration(),
        placement::LinkRestoration::NotRestoring
    );
}

/// A kernel wait is named with the process, its stage and its threads, keeps
/// the moment it was first found while the stage is unchanged, and reads as
/// a recovery a caller may wait on: it ends when the host's storage
/// returns, with nothing further asked of the runner.
#[test]
fn a_kernel_wait_names_its_threads_and_is_a_recovery_in_progress() {
    use cairn_common::executor_protocol::KernelWaitStage;
    use cairn_common::process_identity::KernelThreadStates;
    let syncing = KernelThreadStates {
        leader: Some('S'),
        threads: 7,
        uninterruptible: vec![(4243, Some("ext4_sync_file".into()))],
    };
    let fleet = Fleet::default();
    fleet.declare_colocated_kernel_wait(4242, KernelWaitStage::Starting, syncing.clone());
    let first = fleet.colocated_supervision().kernel_wait.unwrap();
    fleet.declare_colocated_kernel_wait(4242, KernelWaitStage::Starting, syncing.clone());
    assert_eq!(
        fleet
            .colocated_supervision()
            .kernel_wait
            .map(|wait| wait.since_unix_ms),
        Some(first.since_unix_ms)
    );
    let evidence = fleet.colocated_substrate().unwrap();
    assert_eq!(evidence.state, ExecutorSubstrateState::KernelWait);
    let diagnostic = evidence.diagnostic.unwrap();
    assert!(
        diagnostic.starts_with("waiting on the kernel: the starting local executor pid 4242 has 1 of 7 thread(s) in uninterruptible sleep"),
        "{diagnostic}"
    );
    assert!(
        diagnostic.contains("4243 in ext4_sync_file"),
        "{diagnostic}"
    );
    assert!(diagnostic.contains("budget is paused"), "{diagnostic}");
    assert_eq!(
        fleet.link_restoration(),
        placement::LinkRestoration::Restoring
    );

    fleet.declare_colocated_kernel_wait(4242, KernelWaitStage::Killed, syncing);
    let diagnostic = fleet.colocated_substrate().unwrap().diagnostic.unwrap();
    assert!(
        diagnostic.starts_with("killed, waiting on the kernel: "),
        "{diagnostic}"
    );
    assert!(diagnostic.contains("no successor starts"), "{diagnostic}");

    fleet.release_colocated_kernel_wait();
    assert_eq!(fleet.colocated_supervision().kernel_wait, None);
    fleet.declare_colocated_kernel_wait(
        4242,
        KernelWaitStage::Killed,
        KernelThreadStates {
            leader: Some('Z'),
            threads: 2,
            uninterruptible: vec![(4243, None)],
        },
    );
    fleet.clear_colocated_substrate();
    assert_eq!(fleet.colocated_supervision().kernel_wait, None);
}

/// A held generation refuses local placement at once, naming the holder,
/// instead of parking the request on something no progress of ours ends.
#[tokio::test]
async fn a_held_generation_refuses_local_placement_with_its_holder() {
    let temp = tempfile::tempdir().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    orch.fleet.declare_colocated_generation_held(held_by(12184));
    let mut request = provenance_request();
    request.request_id = "held".into();
    request.repository = RepositoryLocator::ScratchOnly {
        owner_id: request.project_id.clone(),
    };
    request.wait_horizon_unix_ms = unix_time_ms() + 60_000;
    let started = std::time::Instant::now();
    let outcome = orch.fleet.submit(&orch, request).await;

    let CellOutcome::Unavailable { diagnostic, .. } = outcome else {
        panic!("a held generation refuses: {outcome:?}");
    };
    assert!(diagnostic.contains("pid 12184"), "{diagnostic}");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(10),
        "the request was parked rather than refused"
    );
}

/// Work only a machine whose room a loading model holds could take waits
/// on the model rather than being refused, placement's model pass hears of
/// the refusal on that machine (and only that machine), and the room
/// returning places the work there.
#[tokio::test]
async fn work_refused_a_model_s_room_waits_reports_the_refusal_and_places_when_it_returns() {
    let pool = Fleet::default();
    let (sender, _colocated) = mpsc::unbounded_channel();
    pool.attach_executor(sender);
    let (generation, _remote) = residency_queue_attach(&pool, "remote");
    pool.announce_room_for_test("remote", generation);
    let remote_name = pool.executor_public_name("remote").unwrap();
    let mut request = spillable_request();
    request.repository = RepositoryLocator::ScratchOnly {
        owner_id: request.project_id.clone(),
    };
    request.resource_reservation = demand(20, ResourceReservationSource::Declared);
    let mut policy = ActivePlacementPolicy::default_profile();
    policy
        .profile
        .routes
        .insert(request.placement_work_class, PlacementStance::RemoteOnly);
    // Fresh against the clock admission reads, so the machine is measured
    // rather than blind.
    measured_at(
        pool.lock_connections().get_mut("remote").unwrap(),
        0.1,
        110 << 30,
        500 << 30,
        unix_time_ms(),
    );
    pool.set_model_residents(HashMap::from([(
        remote_name.clone(),
        vec![model_resident("reserving", 100)],
    )]));
    let (entry, _waiter) = pool
        .queue
        .enqueue(
            queue::NewEntry {
                request: request.clone(),
                admission_kind: CellAdmissionKind::Command,
                interactive: false,
                execution_residency: None,
                standing: false,
            },
            unix_time_ms(),
        )
        .unwrap();

    let waiting = pool.clone();
    let placing = tokio::spawn({
        let request = request.clone();
        let entry = entry.clone();
        async move {
            tokio::time::timeout(
                Duration::from_secs(5),
                assign(&waiting, &entry, &request, &policy, None),
            )
            .await
        }
    });
    let deadline = Instant::now() + Duration::from_secs(2);
    while pool.models.refused.lock().unwrap().is_empty() {
        assert!(
            Instant::now() < deadline,
            "work refused the model's room is reported for eviction"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!placing.is_finished(), "waiting, not refused");
    assert_eq!(
        *pool.models.refused.lock().unwrap(),
        std::collections::BTreeSet::from([remote_name.clone()]),
        "only the machine that refused it"
    );

    pool.set_model_residents(HashMap::new());
    let placement = placing
        .await
        .unwrap()
        .expect("the room returning wakes the wait")
        .expect("the machine takes the work");
    assert_eq!(placement.selected.executor_id, "remote");
}

/// Work whose memory was never measured, offered only a machine whose
/// loading model has room still to take, is refused that room (it fails
/// closed) and waits, without the refusal being reported for eviction.
/// The model turning healthy places it there.
#[tokio::test]
async fn unmeasured_work_waits_out_a_loading_model_and_places_once_it_is_healthy() {
    let pool = Fleet::default();
    let (sender, _colocated) = mpsc::unbounded_channel();
    pool.attach_executor(sender);
    let (generation, _remote) = residency_queue_attach(&pool, "remote");
    pool.announce_room_for_test("remote", generation);
    let remote_name = pool.executor_public_name("remote").unwrap();
    let mut request = spillable_request();
    request.repository = RepositoryLocator::ScratchOnly {
        owner_id: request.project_id.clone(),
    };
    request.resource_reservation = demand(0, ResourceReservationSource::Unmeasured);
    let mut policy = ActivePlacementPolicy::default_profile();
    policy
        .profile
        .routes
        .insert(request.placement_work_class, PlacementStance::RemoteOnly);
    measured_at(
        pool.lock_connections().get_mut("remote").unwrap(),
        0.1,
        110 << 30,
        500 << 30,
        unix_time_ms(),
    );
    pool.set_model_residents(HashMap::from([(
        remote_name.clone(),
        vec![model_resident("reserving", 100)],
    )]));
    let (entry, _waiter) = pool
        .queue
        .enqueue(
            queue::NewEntry {
                request: request.clone(),
                admission_kind: CellAdmissionKind::Command,
                interactive: false,
                execution_residency: None,
                standing: false,
            },
            unix_time_ms(),
        )
        .unwrap();

    let waiting = pool.clone();
    let placing = tokio::spawn({
        let request = request.clone();
        let entry = entry.clone();
        async move {
            tokio::time::timeout(
                Duration::from_secs(5),
                assign(&waiting, &entry, &request, &policy, None),
            )
            .await
        }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!placing.is_finished(), "waiting on the model, not placed");
    assert!(
        pool.models.refused.lock().unwrap().is_empty(),
        "a guess never asks for the model to be evicted"
    );

    pool.set_model_residents(HashMap::from([(
        remote_name,
        vec![model_resident("healthy", 100)],
    )]));
    let placement = placing
        .await
        .unwrap()
        .expect("the model turning healthy wakes the wait")
        .expect("the machine takes the work beside it");
    assert_eq!(placement.selected.executor_id, "remote");
}

/// A machine declines, announces a new room, and is offered the request
/// again, but the requester's horizon arrives while that re-offer is out. The
/// refusal still names the decline, and the newest placement decision for
/// the request is that same refusal carrying the same decline, not the
/// still-selected offer.
#[tokio::test]
async fn a_horizon_that_arrives_during_a_re_offer_records_the_refusal_it_returns() {
    let temp = tempfile::tempdir().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    let (sender, mut executor) = mpsc::unbounded_channel();
    let generation = orch.fleet.attach_announced_executor(sender);
    let detail = "project p is provisioning for request holder, 1s ago";
    let fleet = orch.fleet.clone();
    let machine = tokio::spawn(async move {
        let Some(ExecutorMessage::Offer { entry_id, .. }) = next_work_frame(&mut executor).await
        else {
            panic!("the request is offered");
        };
        fleet.handle_executor_message(
            COLOCATED_EXECUTOR_ID,
            generation,
            ExecutorMessage::Decline {
                entry_id,
                reason: DeclineReason::ProjectProvisioning,
                detail: Some(detail.into()),
                disk: None,
            },
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        // The decline stands under the figures it was made against; new
        // figures are what make the machine worth asking again.
        fleet.announce_room_with_memory_for_test(
            COLOCATED_EXECUTOR_ID,
            generation,
            Some((256 << 30, 512 << 30)),
        );
        // The re-offer arrives and is never answered.
        assert!(matches!(
            next_work_frame(&mut executor).await,
            Some(ExecutorMessage::Offer { .. })
        ));
        while next_work_frame(&mut executor).await.is_some() {}
    });
    let mut request = provenance_request();
    request.request_id = "reoffer-expired".into();
    request.repository = RepositoryLocator::ScratchOnly {
        owner_id: request.project_id.clone(),
    };
    // Long enough for the re-offer to go out before the horizon arrives.
    request.wait_horizon_unix_ms = unix_time_ms() + 2_500;
    let outcome = orch.fleet.submit(&orch, request).await;
    machine.abort();

    let CellOutcome::Unavailable { diagnostic, .. } = outcome else {
        panic!("a request whose horizon passed is refused: {outcome:?}");
    };
    assert!(diagnostic.contains("ProjectProvisioning"), "{diagnostic}");
    assert!(diagnostic.contains(detail), "{diagnostic}");
    let newest = orch
        .fleet
        .recent_placements
        .lock()
        .unwrap()
        .iter()
        .rev()
        .find(|decision| decision.request_id == "reoffer-expired")
        .cloned()
        .expect("the request has placement decisions");
    let PlacementOutcome::Refused {
        diagnostic: recorded,
    } = &newest.outcome
    else {
        panic!("the newest decision is the refusal: {newest:?}");
    };
    assert_eq!(recorded, &diagnostic);
    assert!(
        newest.rejected.iter().any(|rejection| matches!(
            &rejection.reason,
            PlacementRejectionReason::Declined { detail: Some(said), .. } if said == detail
        )),
        "{newest:?}"
    );
    assert!(
        !newest.binding.is_empty(),
        "the refusal carries its binding chain"
    );
}

/// An attached machine on the platform asked for makes "no enrolled
/// machine on that platform exists" false, so it is not said.
#[test]
fn an_attached_machine_on_the_platform_is_not_called_absent() {
    let mut request = targeted_request("macos");
    request.executor = Some(ExecutorSelector {
        os: Some("macos".into()),
        ..ExecutorSelector::default()
    });
    assert_eq!(
        absent_enrolled_inventory(&HashMap::new(), &[], &request),
        "No enrolled machine on macos exists."
    );
    let connections = HashMap::from([fleet_entry("bgsrv-mac", "macos", 0, &[])]);
    assert_eq!(absent_enrolled_inventory(&connections, &[], &request), "");
}

/// `executor-windows-check`'s placement question, against a fleet that has
/// its colocated executor and a Linux machine attached and nothing on Windows.
fn windows_check_fleet() -> Fleet {
    let fleet = Fleet::default();
    let mut connections = fleet.lock_connections();
    connections.extend([
        fleet_entry(COLOCATED_EXECUTOR_ID, "macos", 0, &[]),
        fleet_entry("bgsrv-ub", "linux", 0, &[]),
    ]);
    drop(connections);
    fleet
}

fn rust_selector() -> ExecutorSelector {
    ExecutorSelector {
        required_toolchains: vec!["rust".into()],
        ..ExecutorSelector::default()
    }
}

#[test]
fn a_platform_no_known_machine_runs_is_unplaceable() {
    let fleet = windows_check_fleet();
    let reason = fleet
        .unplaceable_reason(Some(&rust_selector()), &["windows".to_string()])
        .expect("no machine on windows is attached or enrolled");
    assert!(
        reason.contains("toolchains rust on windows"),
        "the reason names what was asked for: {reason}"
    );
    assert!(
        reason.contains("bgsrv-ub (linux; rust)"),
        "the reason names what the fleet has instead: {reason}"
    );
}

/// An enrolled machine that is not attached is a wait, not an absence: what it
/// will advertise is unknown until it attaches.
#[test]
fn an_enrolled_but_detached_machine_on_the_platform_is_a_wait() {
    let fleet = windows_check_fleet();
    fleet.declare_enrolled_remote("bgsrv-win", "bgsrv-win", "windows", "x86_64");
    assert_eq!(
        fleet.unplaceable_reason(Some(&rust_selector()), &["windows".to_string()]),
        None
    );
}

#[test]
fn an_attached_machine_on_the_platform_without_the_toolchain_leaves_it_unplaceable() {
    let fleet = windows_check_fleet();
    let (id, mut windows) = fleet_entry("bgsrv-win", "windows", 0, &[]);
    Arc::make_mut(&mut windows.advertisement)
        .capabilities
        .toolchains
        .clear();
    fleet.lock_connections().insert(id, windows);
    let reason = fleet
        .unplaceable_reason(Some(&rust_selector()), &["windows".to_string()])
        .expect("the only windows machine advertises no rust");
    assert!(
        reason.contains("bgsrv-win (windows; no toolchains)"),
        "{reason}"
    );
}

#[test]
fn an_attached_machine_that_satisfies_the_lane_makes_it_placeable() {
    let fleet = windows_check_fleet();
    let (id, windows) = fleet_entry("bgsrv-win", "windows", 0, &[]);
    fleet.lock_connections().insert(id, windows);
    assert_eq!(
        fleet.unplaceable_reason(Some(&rust_selector()), &["windows".to_string()]),
        None
    );
}

#[test]
fn untargeted_work_is_never_unplaceable() {
    let fleet = Fleet::default();
    assert_eq!(fleet.unplaceable_reason(None, &[]), None);
    assert_eq!(
        fleet.unplaceable_reason(Some(&ExecutorSelector::default()), &[]),
        None
    );
}

/// Before the runner's own executor first attaches, the runner's platform is
/// still reachable: a restart's attach race is not a gap in the fleet.
#[test]
fn the_colocated_executor_counts_on_its_platform_before_it_attaches() {
    let fleet = Fleet::default();
    let host = std::env::consts::OS.to_string();
    assert_eq!(
        fleet.unplaceable_reason(Some(&rust_selector()), std::slice::from_ref(&host)),
        None
    );
    let elsewhere = if host == "windows" {
        "linux"
    } else {
        "windows"
    };
    assert!(fleet
        .unplaceable_reason(Some(&rust_selector()), &[elsewhere.to_string()])
        .is_some());
}

/// A withdrawal is a verdict about the session that refused: it lapses when
/// the machine reattaches under another generation, when it is not attached,
/// and after the withdrawal window, whichever comes first.
#[test]
fn a_home_withdrawal_lapses_with_the_session_or_the_window() {
    let home = WithdrawnHome {
        executor_id: "bgsrv-win".into(),
        generation: 3,
        withdrawn_at_unix_ms: 1_000,
    };
    assert!(home.stands(Some(3), 1_000));
    assert!(home.stands(Some(3), 1_000 + HOME_WITHDRAWAL_MS - 1));
    assert!(!home.stands(Some(3), 1_000 + HOME_WITHDRAWAL_MS));
    assert!(
        !home.stands(Some(4), 1_000),
        "a reattached machine is re-measured"
    );
    assert!(
        !home.stands(None, 1_000),
        "an absent machine is placed nowhere anyway"
    );
}

/// The acquisition a machine is offered, claimed through to its grant.
async fn residency_queue_claimed(
    pool: &Fleet,
    executor: &str,
    generation: u64,
    receiver: &mut mpsc::UnboundedReceiver<ExecutorMessage>,
) -> (String, ResidencyAcquireRequest) {
    let entry = residency_queue_offer(receiver).await;
    pool.handle_executor_message(
        executor,
        generation,
        ExecutorMessage::Claim {
            entry_id: entry.clone(),
        },
    );
    residency_queue_grant(receiver, Some(&entry)).await
}

/// A machine takes the offer it is sent and provisions the home.
async fn residency_queue_accepts(
    pool: &Fleet,
    executor: &str,
    generation: u64,
    receiver: &mut mpsc::UnboundedReceiver<ExecutorMessage>,
) {
    let (correlation, granted) =
        residency_queue_claimed(pool, executor, generation, receiver).await;
    residency_queue_reply(pool, executor, generation, correlation, &granted);
}

/// A machine takes the offer it is sent and answers the grant with a failure.
async fn residency_queue_refuses(
    pool: &Fleet,
    executor: &str,
    generation: u64,
    receiver: &mut mpsc::UnboundedReceiver<ExecutorMessage>,
    kind: ResidencyFailureKind,
    refusal: &str,
) {
    let (correlation, _) = residency_queue_claimed(pool, executor, generation, receiver).await;
    pool.handle_executor_message(
        executor,
        generation,
        ExecutorMessage::ResidencyResponse {
            correlation_id: correlation,
            result: residency_core_failure(kind, refusal, None),
        },
    );
}

fn assert_never_offered(receiver: &mut mpsc::UnboundedReceiver<ExecutorMessage>, machine: &str) {
    while let Ok(frame) = receiver.try_recv() {
        assert!(
            !matches!(
                frame,
                ExecutorMessage::Offer { .. } | ExecutorMessage::ResidencyRequest { .. }
            ),
            "{machine} must not be offered this acquisition: {frame:?}"
        );
    }
}

fn residency_queue_untargeted(job: &str) -> ResidencyAcquireRequest {
    let mut request = residency_queue_request(job);
    request.executor = None;
    request
}

fn on_platform(os: &str) -> ExecutorSelector {
    ExecutorSelector {
        os: Some(os.into()),
        ..Default::default()
    }
}

/// cairn/5875's first specimen: the idle Windows and Linux machines both
/// sort ahead of the Mac, and neither is ever offered the home.
#[tokio::test(flavor = "multi_thread")]
async fn a_platform_selector_never_acquires_a_home_on_another_platform() {
    residency_queue_scenario(async {
        let temp = tempfile::tempdir().unwrap();
        let orch = test_orchestrator(temp.path()).await;
        let (_, mut windows) = residency_queue_attach_on(&orch.fleet, "a-windows", "windows");
        let (_, mut linux) = residency_queue_attach_on(&orch.fleet, "b-linux", "linux");
        let (mac_generation, mut mac) = residency_queue_attach_on(&orch.fleet, "z-macos", "macos");
        let mut request = residency_queue_request("thread");
        request.executor = Some(on_platform("macos"));
        let (placed, ()) = tokio::join!(
            orch.fleet.operate_residency(
                &orch,
                ResidencyOperation::Acquire {
                    request: request.clone()
                }
            ),
            residency_queue_accepts(&orch.fleet, "z-macos", mac_generation, &mut mac)
        );
        assert!(
            matches!(placed, ResidencyResult::State { .. }),
            "the Mac takes the home: {placed:?}"
        );
        assert_never_offered(&mut windows, "the readier Windows machine");
        assert_never_offered(&mut linux, "the readier Linux machine");
        assert_eq!(
            orch.fleet.residency_route_executor(&request.holder),
            Some("z-macos".to_string())
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_selector_no_machine_satisfies_is_refused_by_name_and_acquires_nothing() {
    residency_queue_scenario(async {
        let temp = tempfile::tempdir().unwrap();
        let orch = test_orchestrator(temp.path()).await;
        let (_, mut windows) = residency_queue_attach_on(&orch.fleet, "a-windows", "windows");
        let mut request = residency_queue_request("thread");
        request.executor = Some(on_platform("macos"));
        let refused = orch
            .fleet
            .operate_residency(
                &orch,
                ResidencyOperation::Acquire {
                    request: request.clone(),
                },
            )
            .await;
        let ResidencyResult::Failed {
            kind, cell_outcome, ..
        } = &refused
        else {
            panic!("nothing can satisfy the selector: {refused:?}");
        };
        let Some(CellOutcome::Unavailable { diagnostic, .. }) = cell_outcome.as_deref() else {
            panic!("the refusal carries the placement sentence: {refused:?}");
        };
        assert!(
            diagnostic.contains("macos"),
            "the refusal names the selector: {diagnostic}"
        );
        assert_eq!(
            placement::classify_residency_failure(
                kind,
                cell_outcome.as_deref(),
                placement::LinkRestoration::NotRestoring
            ),
            placement::PlacementVerdict::Structural,
            "no amount of waiting changes this answer"
        );
        assert_never_offered(&mut windows, "a machine the selector refuses");
        assert!(
            orch.fleet.residency_routes.lock().unwrap().is_empty(),
            "nothing was acquired"
        );
    })
    .await;
}

/// cairn/5875's third requirement, through one `operate_residency`.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_first_grant_continues_to_the_next_machine_in_the_same_call() {
    residency_queue_scenario(async {
        let temp = tempfile::tempdir().unwrap();
        let orch = test_orchestrator(temp.path()).await;
        let (broken_generation, mut broken) = residency_queue_attach(&orch.fleet, "a-broken");
        let (healthy_generation, mut healthy) = residency_queue_attach(&orch.fleet, "b-healthy");
        let request = residency_queue_untargeted("thread");
        let (placed, ()) = tokio::join!(
            orch.fleet.operate_residency(
                &orch,
                ResidencyOperation::Acquire {
                    request: request.clone()
                }
            ),
            async {
                residency_queue_refuses(
                    &orch.fleet,
                    "a-broken",
                    broken_generation,
                    &mut broken,
                    ResidencyFailureKind::Persistence,
                    "provision residency checkout: Access is denied",
                )
                .await;
                residency_queue_accepts(&orch.fleet, "b-healthy", healthy_generation, &mut healthy)
                    .await;
            }
        );
        assert!(
            matches!(placed, ResidencyResult::State { .. }),
            "the same call lands on the next machine: {placed:?}"
        );
        assert_eq!(
            orch.fleet.withdrawn_home_executors(&request.holder),
            vec!["a-broken".to_string()]
        );
        let routes = orch.fleet.residency_routes.lock().unwrap();
        let holders: Vec<_> = routes
            .values()
            .map(|route| route.executor_id.as_str())
            .collect();
        assert_eq!(
            holders,
            vec!["b-healthy"],
            "one home, on the machine that built it"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn every_machine_refusing_names_every_refusal_promptly() {
    residency_queue_scenario(async {
        let temp = tempfile::tempdir().unwrap();
        let orch = test_orchestrator(temp.path()).await;
        let (first_generation, mut first) = residency_queue_attach(&orch.fleet, "a-first");
        let (second_generation, mut second) = residency_queue_attach(&orch.fleet, "b-second");
        let mut request = residency_queue_untargeted("thread");
        request.wait_horizon_unix_ms = unix_time_ms() + 3_600_000;
        let (refused, ()) = tokio::join!(
            orch.fleet.operate_residency(
                &orch,
                ResidencyOperation::Acquire {
                    request: request.clone()
                }
            ),
            async {
                residency_queue_refuses(
                    &orch.fleet,
                    "a-first",
                    first_generation,
                    &mut first,
                    ResidencyFailureKind::Persistence,
                    "first machine's storage",
                )
                .await;
                residency_queue_refuses(
                    &orch.fleet,
                    "b-second",
                    second_generation,
                    &mut second,
                    ResidencyFailureKind::Cleanup,
                    "second machine's storage",
                )
                .await;
            }
        );
        let ResidencyResult::Failed {
            kind, diagnostic, ..
        } = &refused
        else {
            panic!("no machine remains: {refused:?}");
        };
        assert_eq!(
            *kind,
            ResidencyFailureKind::Cleanup,
            "keeps the last answer's kind, so classification is unchanged"
        );
        for named in [
            "a-first",
            "first machine's storage",
            "b-second",
            "second machine's storage",
        ] {
            assert!(diagnostic.contains(named), "names {named}: {diagnostic}");
        }
        // The scenario's own 20 s bound is what holds this to "promptly": the
        // horizon above is an hour, so nothing here waited it out.
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refusal_that_does_not_withdraw_ends_the_acquisition() {
    residency_queue_scenario(async {
        let temp = tempfile::tempdir().unwrap();
        let orch = test_orchestrator(temp.path()).await;
        let (generation, mut first) = residency_queue_attach(&orch.fleet, "a-first");
        let (_, mut second) = residency_queue_attach(&orch.fleet, "b-second");
        let request = residency_queue_untargeted("thread");
        let (refused, ()) = tokio::join!(
            orch.fleet.operate_residency(
                &orch,
                ResidencyOperation::Acquire {
                    request: request.clone()
                }
            ),
            residency_queue_refuses(
                &orch.fleet,
                "a-first",
                generation,
                &mut first,
                ResidencyFailureKind::InvalidDeclaration,
                "the declaration itself is wrong",
            )
        );
        assert!(
            matches!(
                &refused,
                ResidencyResult::Failed {
                    kind: ResidencyFailureKind::InvalidDeclaration,
                    ..
                }
            ),
            "{refused:?}"
        );
        assert_never_offered(&mut second, "the next machine");
        assert!(orch
            .fleet
            .withdrawn_home_executors(&request.holder)
            .is_empty());
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_named_selector_is_refused_without_withdrawing_the_machine() {
    residency_queue_scenario(async {
        let temp = tempfile::tempdir().unwrap();
        let orch = test_orchestrator(temp.path()).await;
        let (generation, mut remote) = residency_queue_attach(&orch.fleet, "remote");
        let (_, mut other) = residency_queue_attach(&orch.fleet, "other");
        // `residency_queue_request` names `remote`.
        let request = residency_queue_request("thread");
        let refusal = "provision residency checkout: Access is denied";
        let (refused, ()) = tokio::join!(
            orch.fleet.operate_residency(
                &orch,
                ResidencyOperation::Acquire {
                    request: request.clone()
                }
            ),
            residency_queue_refuses(
                &orch.fleet,
                "remote",
                generation,
                &mut remote,
                ResidencyFailureKind::Persistence,
                refusal,
            )
        );
        let ResidencyResult::Failed { diagnostic, .. } = &refused else {
            panic!("the machine it named refused: {refused:?}");
        };
        let machine = orch.fleet.executor_name_for_text("remote");
        assert_eq!(
            diagnostic,
            &format!("{machine} refused to provision this job's execution environment: {refusal}"),
            "its own refusal, with nothing withdrawn"
        );
        assert_never_offered(&mut other, "a machine the selector did not name");
        assert!(
            orch.fleet
                .withdrawn_home_executors(&request.holder)
                .is_empty(),
            "one answer must not refuse the caller's own choice for the withdrawal window"
        );
    })
    .await;
}

/// A platform selector narrowed to one machine is the same case as a name:
/// a transient failure there must not refuse every matching batch for the
/// length of a withdrawal without the machine ever being asked again.
#[tokio::test(flavor = "multi_thread")]
async fn a_selector_with_one_matching_machine_asks_it_again_after_a_refusal() {
    residency_queue_scenario(async {
        let temp = tempfile::tempdir().unwrap();
        let orch = test_orchestrator(temp.path()).await;
        let (_, mut linux) = residency_queue_attach_on(&orch.fleet, "a-linux", "linux");
        let (generation, mut mac) = residency_queue_attach_on(&orch.fleet, "z-mac", "macos");
        let mut request = residency_queue_request("thread");
        request.executor = Some(on_platform("macos"));
        let refusal = "provision residency checkout: Access is denied";
        let (refused, ()) = tokio::join!(
            orch.fleet.operate_residency(
                &orch,
                ResidencyOperation::Acquire {
                    request: request.clone()
                }
            ),
            residency_queue_refuses(
                &orch.fleet,
                "z-mac",
                generation,
                &mut mac,
                ResidencyFailureKind::Persistence,
                refusal,
            )
        );
        let ResidencyResult::Failed { diagnostic, .. } = &refused else {
            panic!("the only matching machine refused: {refused:?}");
        };
        let machine = orch.fleet.executor_name_for_text("z-mac");
        assert!(
            diagnostic.contains(&machine) && diagnostic.contains(refusal),
            "names the machine and its words: {diagnostic}"
        );
        assert!(
            !diagnostic.contains("withdrawn"),
            "nothing was withdrawn, so it must not say so: {diagnostic}"
        );
        assert!(orch
            .fleet
            .withdrawn_home_executors(&request.holder)
            .is_empty());

        let (placed, ()) = tokio::join!(
            orch.fleet.operate_residency(
                &orch,
                ResidencyOperation::Acquire {
                    request: request.clone()
                }
            ),
            residency_queue_accepts(&orch.fleet, "z-mac", generation, &mut mac)
        );
        assert!(
            matches!(placed, ResidencyResult::State { .. }),
            "the next acquisition asks the machine again: {placed:?}"
        );
        assert_never_offered(&mut linux, "a machine the selector refuses");
    })
    .await;
}

/// Two machines match: the first refusal steers to the second, and running
/// out leaves the last one askable rather than locking the selector out.
#[tokio::test(flavor = "multi_thread")]
async fn a_selector_that_runs_out_of_machines_names_both_and_keeps_the_last_askable() {
    residency_queue_scenario(async {
        let temp = tempfile::tempdir().unwrap();
        let orch = test_orchestrator(temp.path()).await;
        let (first_generation, mut first) =
            residency_queue_attach_on(&orch.fleet, "a-mac", "macos");
        let (second_generation, mut second) =
            residency_queue_attach_on(&orch.fleet, "b-mac", "macos");
        let mut request = residency_queue_request("thread");
        request.executor = Some(on_platform("macos"));
        let (refused, ()) = tokio::join!(
            orch.fleet.operate_residency(
                &orch,
                ResidencyOperation::Acquire {
                    request: request.clone()
                }
            ),
            async {
                residency_queue_refuses(
                    &orch.fleet,
                    "a-mac",
                    first_generation,
                    &mut first,
                    ResidencyFailureKind::Persistence,
                    "first machine's storage",
                )
                .await;
                residency_queue_refuses(
                    &orch.fleet,
                    "b-mac",
                    second_generation,
                    &mut second,
                    ResidencyFailureKind::Persistence,
                    "second machine's storage",
                )
                .await;
            }
        );
        let ResidencyResult::Failed { diagnostic, .. } = &refused else {
            panic!("no matching machine remains: {refused:?}");
        };
        for named in [
            orch.fleet.executor_name_for_text("a-mac"),
            "first machine's storage".to_string(),
            orch.fleet.executor_name_for_text("b-mac"),
            "second machine's storage".to_string(),
        ] {
            assert!(diagnostic.contains(&named), "names {named}: {diagnostic}");
        }
        assert_eq!(
            orch.fleet.withdrawn_home_executors(&request.holder),
            vec!["a-mac".to_string()],
            "the machine that had a successor is withdrawn; the last one is not"
        );

        let (placed, ()) = tokio::join!(
            orch.fleet.operate_residency(
                &orch,
                ResidencyOperation::Acquire {
                    request: request.clone()
                }
            ),
            residency_queue_accepts(&orch.fleet, "b-mac", second_generation, &mut second)
        );
        assert!(
            matches!(placed, ResidencyResult::State { .. }),
            "{placed:?}"
        );
        assert_never_offered(&mut first, "the machine still withdrawn");
    })
    .await;
}

/// A scratch affinity guess reaches the acquisition as a price and never as
/// a selector, so the machine it favours can refuse and be passed over.
#[tokio::test(flavor = "multi_thread")]
async fn a_scratch_affinity_guess_never_pins_a_home() {
    residency_queue_scenario(async {
        let temp = tempfile::tempdir().unwrap();
        let orch = test_orchestrator(temp.path()).await;
        let (next_generation, mut next) = residency_queue_attach(&orch.fleet, "a-next");
        let (guessed_generation, mut guessed) = residency_queue_attach(&orch.fleet, "z-guessed");
        let request = residency_queue_untargeted("thread");
        let prices = HomePrices {
            home_affinity: None,
            scratch_sync: Some(ScratchSyncEstimate {
                per_executor_ms: std::collections::BTreeMap::from([("z-guessed".to_string(), 0)]),
                uncovered_ms: 60_000,
            }),
        };
        let (placed, ()) = tokio::join!(
            orch.fleet
                .acquire_priced_home(&orch, request.clone(), prices),
            async {
                residency_queue_refuses(
                    &orch.fleet,
                    "z-guessed",
                    guessed_generation,
                    &mut guessed,
                    ResidencyFailureKind::Persistence,
                    "provision residency checkout: Access is denied",
                )
                .await;
                residency_queue_accepts(&orch.fleet, "a-next", next_generation, &mut next).await;
            }
        );
        assert!(
            matches!(placed, ResidencyResult::State { .. }),
            "{placed:?}"
        );
        assert_eq!(
            orch.fleet.withdrawn_home_executors(&request.holder),
            vec!["z-guessed".to_string()]
        );
        assert_eq!(
            orch.fleet.residency_route_executor(&request.holder),
            Some("a-next".to_string())
        );
    })
    .await;
}

/// The refusal and the predicate meet here: a job a guess sends back to the
/// machine that just refused its home would undo the withdrawal.
#[tokio::test(flavor = "multi_thread")]
async fn a_guess_does_not_send_a_job_back_to_the_machine_that_refused_its_home() {
    residency_queue_scenario(async {
        let temp = tempfile::tempdir().unwrap();
        let orch = test_orchestrator(temp.path()).await;
        let (broken_generation, mut broken) = residency_queue_attach(&orch.fleet, "a-broken");
        let request = residency_queue_untargeted("thread");
        let (refused, ()) = tokio::join!(
            orch.fleet.operate_residency(
                &orch,
                ResidencyOperation::Acquire {
                    request: request.clone()
                }
            ),
            residency_queue_refuses(
                &orch.fleet,
                "a-broken",
                broken_generation,
                &mut broken,
                ResidencyFailureKind::Persistence,
                "provision residency checkout: Access is denied",
            )
        );
        assert!(
            matches!(refused, ResidencyResult::Failed { .. }),
            "{refused:?}"
        );
        let (_holder_generation, _holder) = residency_queue_attach(&orch.fleet, "b-holder");

        let holder_on = |executor_id: &str| crate::node_scratch::holders::ScratchHolder {
            process: format!("terminal on {executor_id}"),
            executor_id: executor_id.into(),
            job_id: "thread".into(),
        };
        let machines = vec![
            ("a-broken".to_string(), vec![holder_on("a-broken")]),
            ("b-holder".to_string(), vec![holder_on("b-holder")]),
        ];
        let price = |executor_id: &str| match executor_id {
            "a-broken" => 0,
            _ => 900,
        };
        let name = |executor_id: &str| executor_id.to_string();
        let can_take = |executor_id: &str| {
            orch.fleet
                .scratch_holder_can_take(executor_id, &request.holder)
        };
        let chosen =
            crate::node_scratch::holders::affinity_among(&machines, can_take, price, None, name)
                .unwrap();
        assert_eq!(
            chosen.executor_id.as_deref(),
            Some("b-holder"),
            "the warmer machine refused this job's home, so the guess passes it over"
        );
        let chosen = crate::node_scratch::holders::affinity_among(
            &machines[..1],
            can_take,
            price,
            None,
            name,
        )
        .unwrap();
        assert_eq!(chosen.executor_id, None);
        assert!(
            chosen.note.contains("placed without that affinity"),
            "a guess is filtered, never a refusal: {}",
            chosen.note
        );
        assert!(
            orch.fleet.scratch_holder_can_take(
                "a-broken",
                &ResidencyHolder::Job {
                    job_id: "another-job".into()
                }
            ),
            "the withdrawal is about that job's home, not the machine"
        );
    })
    .await;
}

/// A route can outlive the environment it names: the executor released or
/// reclaimed the residency, and answers a reattachment that it holds
/// nothing to converge on. Reattaching again would get that answer
/// forever, which is how a service's automatic restarts were refused until
/// it failed (cairn/6528). The fleet forgets the route and places the same
/// acquisition through the queue, so it is granted a fresh environment.
#[tokio::test]
async fn residency_reattachment_to_a_released_environment_is_placed_through_the_queue() {
    residency_queue_scenario(async {
        let temp = tempfile::tempdir().unwrap();
        let orch = test_orchestrator(temp.path()).await;
        let (generation, mut remote) = residency_queue_attach(&orch.fleet, "remote");
        let request = residency_queue_request("released");
        let (first, ()) = tokio::join!(
            orch.fleet.operate_residency(
                &orch,
                ResidencyOperation::Acquire {
                    request: request.clone()
                }
            ),
            async {
                let entry = residency_queue_offer(&mut remote).await;
                orch.fleet.handle_executor_message(
                    "remote",
                    generation,
                    ExecutorMessage::Claim {
                        entry_id: entry.clone(),
                    },
                );
                let (correlation, granted) =
                    residency_queue_grant(&mut remote, Some(&entry)).await;
                residency_queue_reply(&orch.fleet, "remote", generation, correlation, &granted);
            }
        );
        assert!(matches!(first, ResidencyResult::State { .. }), "{first:?}");
        assert_eq!(orch.fleet.residency_routes.lock().unwrap().len(), 1);

        let mut again = request.clone();
        again.waiting_since_unix_ms = unix_time_ms();
        again.wait_horizon_unix_ms = unix_time_ms() + 10_000;
        let (second, ()) = tokio::join!(
            orch.fleet.operate_residency(
                &orch,
                ResidencyOperation::Acquire {
                    request: again.clone()
                }
            ),
            async {
                let (correlation, reattach) = residency_queue_grant(&mut remote, None).await;
                assert_eq!(reattach.holder, request.holder);
                orch.fleet.handle_executor_message(
                    "remote",
                    generation,
                    ExecutorMessage::ResidencyResponse {
                        correlation_id: correlation,
                        result: residency_core_failure(
                            ResidencyFailureKind::NotFound,
                            "residency acquisition cannot converge; the runner must place this acquisition through the execution queue",
                            None,
                        ),
                    },
                );
                let entry = residency_queue_offer(&mut remote).await;
                assert!(
                    orch.fleet.residency_routes.lock().unwrap().is_empty(),
                    "the route to the released environment is forgotten"
                );
                orch.fleet.handle_executor_message(
                    "remote",
                    generation,
                    ExecutorMessage::Claim {
                        entry_id: entry.clone(),
                    },
                );
                let (correlation, granted) =
                    residency_queue_grant(&mut remote, Some(&entry)).await;
                residency_queue_reply(&orch.fleet, "remote", generation, correlation, &granted);
            }
        );
        assert!(matches!(second, ResidencyResult::State { .. }), "{second:?}");
        assert_eq!(orch.fleet.residency_routes.lock().unwrap().len(), 1);
        assert_eq!(orch.fleet.queue.len(), 0);
    })
    .await;
}

#[tokio::test]
async fn residency_queue_claim_then_route_persistence_failure_has_no_object_authority() {
    residency_queue_scenario(async {
            let temp = tempfile::tempdir().unwrap();
            let mut orch = test_orchestrator(temp.path()).await;
            let route_parent = temp.path().join("route-parent");
            let route_path = route_parent.join("routes.json");
            orch.fleet = Arc::new(Fleet::with_residency_route_path(route_path));
            let repository_path = temp.path().join("repository");
            std::fs::create_dir_all(&repository_path).unwrap();
            orch.db.local.execute_script(format!(
                "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p', 'default', 'Project', 'p', '{}', 1, 1);",
                repository_path.to_string_lossy()
            )).await.unwrap();
            let (generation, mut remote) = residency_queue_attach(&orch.fleet, "remote");
            let mut request = residency_queue_request("authority");
            request.repository = RepositoryLocator::ColocatedPath {
                project_id: "p".into(), repository_id: "p".into(),
                absolute_path: repository_path.to_string_lossy().into_owned(),
            };
            let coordinate = ObjectTransferCoordinate {
                repository: request.repository.identity(),
                request_id: residency_queue_entry_id(&request.holder),
                attempt_id: RESIDENCY_ACQUIRE_ATTEMPT_ID.into(),
                executor_id: "remote".into(), connection_generation: generation,
            };
            let probe = residency_placement_request(&request);
            orch.object_plane.authorize_request(&probe, "remote", generation);
            assert!(orch.object_plane.authorizes(&coordinate, "base"), "the exact coordinate must detect authorization");
            orch.object_plane.revoke_request(&coordinate.request_id, &coordinate.attempt_id, "remote", generation);
            let (_machine_home, machine) = claiming_machine();
            let (result, (entry, summary)) = tokio::join!(
                orch.fleet.operate_residency(&orch, ResidencyOperation::Acquire { request }),
                async {
                    let (entry, summary) = match residency_queue_frame(&mut remote).await {
                        ExecutorMessage::Offer { entry_id, summary } => (entry_id, summary),
                        other => panic!("fresh acquisition must offer before granting: {other:?}"),
                    };
                    assert!(!orch.object_plane.authorizes(&coordinate, "base"));
                    machine_claims(&machine, &entry, &summary).await;
                    // Break persistence only after all placement preparation and
                    // route resolution succeeded, before the real Claim arrives.
                    std::fs::write(&route_parent, "not a directory").unwrap();
                    orch.fleet.handle_executor_message("remote", generation, ExecutorMessage::Claim { entry_id: entry.clone() });
                    assert!(matches!(orch.fleet.queue.state_of(&entry), Some(queue::EntryState::Claimed { .. })));
                    (entry, summary)
                }
            );
            assert!(matches!(result, ResidencyResult::Failed { kind: ResidencyFailureKind::Persistence, ref diagnostic, .. } if diagnostic.contains("persist pending residency route authority")), "failure must be after a real claim: {result:?}");
            assert!(!orch.object_plane.authorizes(&coordinate, "base"), "a claim whose route cannot persist grants no object authority");
            assert_eq!(orch.fleet.queue.len(), 0);
            assert!(orch.fleet.pending_residency.lock().unwrap().is_empty());
            let mut withdrawn = false;
            while let Ok(frame) = remote.try_recv() {
                assert!(!matches!(frame, ExecutorMessage::ResidencyRequest { .. }), "failed persistence must not grant: {frame:?}");
                assert!(!matches!(frame, ExecutorMessage::Cancel { .. }), "an ungranted claim is withdrawn, not cancelled: {frame:?}");
                if matches!(&frame, ExecutorMessage::Lost { entry_id } if *entry_id == entry) {
                    machine_hears(&machine, frame).await;
                    withdrawn = true;
                }
            }
            assert!(withdrawn, "the machine is told its claim is withdrawn with Lost");
            assert_machine_released(&machine, &summary).await;
        }).await;
}

// --- cairn/4798: placement honors what preparation and the build need ---

const LINUX_PREREQUISITES: &str = "scripts/setup-self-hosted-runner-linux.sh";
const SETUP: &str = "setup-bun-install";

fn advertising(entry: &mut ExecutorConnectionState, toolchains: &[&str]) {
    Arc::make_mut(&mut entry.advertisement)
        .capabilities
        .toolchains = toolchains
        .iter()
        .map(|toolchain| toolchain.to_string())
        .collect();
}

fn project_needs(setup_toolchains: &[&str], scripts: &[&str]) -> ProjectPlacementRequirements {
    ProjectPlacementRequirements {
        setup_toolchains: setup_toolchains.iter().map(|t| t.to_string()).collect(),
        prerequisite_scripts: scripts.iter().map(|s| s.to_string()).collect(),
        setup_identity: SETUP.into(),
        capabilities: Vec::new(),
    }
}

fn live_checkout() -> RepositoryLocator {
    RepositoryLocator::ExistingCheckout {
        project_id: "p".into(),
        repository_id: "repo".into(),
        absolute_path: "/repo".into(),
    }
}

fn not_found(command: &str, program: &str) -> CellOutcome {
    CellOutcome::Unavailable {
        reason: CellUnavailableReason::PreparationCommandNotFound {
            command: command.into(),
            program: program.into(),
            exit_code: 127,
        },
        diagnostic: format!(
            "cell preparation command failed: {command}\n/bin/sh: 1: {program}: not found"
        ),
    }
}

fn record(pool: &Fleet, id: &str) -> bool {
    pool.record_preparation_refusal(id, "p", SETUP, "bun install", "bun", "bun: not found")
}

fn home_route(pool: &Fleet, id: &str, request: &ResidencyAcquireRequest) {
    pool.residency_routes.lock().unwrap().insert(
        (id.to_string(), request.holder.storage_key()),
        ResidencyRoute {
            holder: request.holder.clone(),
            repository: request.repository.clone(),
            executor_id: id.to_string(),
            pending: false,
        },
    );
}

fn prerequisites(
    status: cairn_common::executor_protocol::BuildPrerequisiteStatus,
) -> cairn_common::executor_protocol::BuildPrerequisiteCheck {
    cairn_common::executor_protocol::BuildPrerequisiteCheck {
        script: format!("{LINUX_PREREQUISITES} --check"),
        status,
        detail: "Missing Linux self-hosted runner prerequisites on bbs: cmake libclang-dev\nProvision bbs once with: sudo scripts/setup-self-hosted-runner-linux.sh".into(),
    }
}

fn probed(entry: &mut ExecutorConnectionState, bun_detected: bool) {
    Arc::make_mut(&mut entry.advertisement)
        .capabilities
        .toolchain_detection = Some(cairn_common::executor_protocol::ToolchainDetection {
        account: "dev".into(),
        home: "/home/dev".into(),
        probes: vec![cairn_common::executor_protocol::ToolchainProbe {
            toolchain: "bun".into(),
            command: "bun --version".into(),
            detected: bun_detected,
            resolved_path: bun_detected.then(|| "/home/dev/.bun/bin/bun".into()),
            detail: if bun_detected {
                "1.2.3".into()
            } else {
                "could not run: No such file or directory".into()
            },
            build_sha256: None,
            ..Default::default()
        }],
    });
    advertising(entry, if bun_detected { &["bun"] } else { &[] });
}

/// A machine whose only npm record is `probe`, advertising it exactly when the
/// record is a verdict that it is there.
fn npm_probed(
    entry: &mut ExecutorConnectionState,
    probe: cairn_common::executor_protocol::ToolchainProbe,
) {
    let advertised: &[&str] = if probe.detected { &["npm"] } else { &[] };
    Arc::make_mut(&mut entry.advertisement)
        .capabilities
        .toolchain_detection = Some(cairn_common::executor_protocol::ToolchainDetection {
        account: "dev".into(),
        home: "/home/dev".into(),
        probes: vec![probe],
    });
    advertising(entry, advertised);
}

fn npm_record(
    detected: bool,
    measured: bool,
    timed_out: bool,
) -> cairn_common::executor_protocol::ToolchainProbe {
    let cause = "did not finish within 30s and was killed";
    cairn_common::executor_protocol::ToolchainProbe {
        toolchain: "npm".into(),
        command: "npm --version".into(),
        detected,
        resolved_path: Some("/home/dev/.cairn-exec/bin/npm".into()),
        detail: if detected {
            "10.9.2 (manifest)".into()
        } else if measured {
            "exited 127: npm: not found".into()
        } else {
            cause.into()
        },
        measured_at_unix_ms: measured.then_some(NOW - 3_600_000),
        not_measured: timed_out.then(|| {
            cairn_common::executor_protocol::ToolchainProbeUnmeasured {
                at_unix_ms: NOW - 600_000,
                cause: cause.into(),
            }
        }),
        ..Default::default()
    }
}

/// The infra-stacks lane on bginf-sp1: a lane that names npm takes a machine
/// whose npm is verified, and one whose npm was verified and whose latest
/// probe timed out; it refuses one that measured npm absent, and one no probe
/// has completed on, each refusal naming the state it acted on.
#[test]
fn a_lane_requiring_a_toolchain_reads_its_probe_state() {
    let selector = ExecutorSelector {
        os: Some("linux".into()),
        required_toolchains: vec!["npm".into()],
        ..ExecutorSelector::default()
    };
    let (_, mut entry) = fleet_entry("bginf-sp1", "linux", 0, &[]);

    npm_probed(&mut entry, npm_record(true, true, false));
    assert!(selector_shortfall(&entry, &selector).is_none());

    npm_probed(&mut entry, npm_record(true, true, true));
    assert!(
        selector_shortfall(&entry, &selector).is_none(),
        "a timeout after a verdict keeps the machine eligible"
    );

    npm_probed(&mut entry, npm_record(false, true, false));
    let absent = selector_shortfall(&entry, &selector)
        .expect("a measured absence refuses")
        .describe();
    assert_eq!(
        absent,
        "does not advertise npm (npm absent (`npm --version`: exited 127: npm: not found))"
    );

    npm_probed(&mut entry, npm_record(false, false, true));
    let gap = selector_shortfall(&entry, &selector)
        .expect("a lane that names npm needs it verified")
        .describe();
    assert!(
        gap.starts_with("does not advertise npm (npm not measured since "),
        "{gap}"
    );
    assert!(
        gap.ends_with("(`npm --version` did not finish within 30s and was killed))"),
        "{gap}"
    );
    assert!(!gap.contains("absent"), "{gap}");
}

/// The census names the state too, so a machine whose npm probe timed out
/// reads as one in the unplaceable diagnostic rather than as a machine
/// without npm.
#[test]
fn the_unplaceable_census_names_a_required_toolchains_probe_state() {
    let fleet = Fleet::default();
    let (id, mut entry) = fleet_entry("bginf-sp1", "linux", 0, &[]);
    npm_probed(&mut entry, npm_record(false, false, true));
    fleet.lock_connections().insert(id, entry);
    // An attached colocated executor without npm, so the runner's own
    // platform is not counted as reachable before its executor attaches,
    // which on a Linux host would make any linux lane placeable.
    let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, std::env::consts::OS, 0, &[]);
    local.colocated = true;
    advertising(&mut local, &[]);
    fleet.lock_connections().insert(local_id, local);
    let selector = ExecutorSelector {
        required_toolchains: vec!["npm".into()],
        ..ExecutorSelector::default()
    };
    let reason = fleet
        .unplaceable_reason(Some(&selector), &["linux".to_string()])
        .expect("no machine has verified npm");
    assert!(
        reason.contains(
            "bginf-sp1 (linux; no toolchains; does not advertise npm (npm not measured since "
        ),
        "{reason}"
    );
}

/// Setup is the one consumer that learns from its own failure (tenet 951), so
/// a setup toolchain no probe has completed on is a gap the machine stays
/// eligible across; a measured absence still refuses, naming its evidence.
#[test]
fn a_setup_toolchain_that_was_never_measured_is_a_gap_not_a_refusal() {
    let (_, mut entry) = fleet_entry("bginf-sp1", "linux", 0, &[]);
    let mut request = spillable_request();
    request.project_requirements = project_needs(&["npm"], &[]).into();

    npm_probed(&mut entry, npm_record(false, false, true));
    assert!(!matches!(
        candidate_rejection(&entry, PlacementScope::of(&request), NOW),
        Some(PlacementRejectionReason::PreparationToolchainMissing { .. })
    ));

    npm_probed(&mut entry, npm_record(true, true, true));
    assert!(!matches!(
        candidate_rejection(&entry, PlacementScope::of(&request), NOW),
        Some(PlacementRejectionReason::PreparationToolchainMissing { .. })
    ));

    npm_probed(&mut entry, npm_record(false, true, false));
    let refusal = candidate_rejection(&entry, PlacementScope::of(&request), NOW)
        .expect("a measured absence refuses");
    assert_eq!(
        refusal.describe(),
        "project setup for p requires npm; this machine advertises none (npm absent (`npm --version`: exited 127: npm: not found))"
    );
}

#[test]
fn a_structural_project_mismatch_is_not_masked_by_a_transient_hold_or_link() {
    let (_, mut entry) = fleet_entry("fedora", "linux", 0, &[]);
    let (sender, receiver) = mpsc::unbounded_channel();
    drop(receiver);
    entry.sender = sender;
    let mut request = spillable_request();
    request.project_requirements = project_needs(&["bun"], &[]).into();
    assert!(matches!(
        candidate_rejection(&entry, PlacementScope::of(&request), NOW),
        Some(PlacementRejectionReason::PreparationToolchainMissing { .. })
    ));
    runtime_update_hold_fixture(&mut entry, true);
    assert!(matches!(
        candidate_rejection(&entry, PlacementScope::of(&request), NOW),
        Some(PlacementRejectionReason::PreparationToolchainMissing { .. })
    ));
    Arc::make_mut(&mut entry.advertisement)
        .capabilities
        .projects_served = vec!["other".into()];
    assert!(matches!(
        candidate_rejection(&entry, PlacementScope::of(&request), NOW),
        Some(PlacementRejectionReason::ProjectUnavailable { .. })
    ));
}

/// A thin fleet whose only machine cannot prepare the project, for work
/// that cannot leave it: refused now, naming the requirement, rather than
/// waiting out a horizon on a fact time does not change.
#[test]
fn immobile_work_on_a_machine_that_cannot_prepare_it_is_refused_at_once() {
    let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]);
    local.colocated = true;
    advertising(&mut local, &[]);
    let connections = HashMap::from([(local_id, local)]);
    let mut request = spillable_request();
    request.placement_mobility = PlacementMobility::PinnedOrColocated;
    request.project_requirements = project_needs(&["bun"], &[]).into();

    let refusal = place(&connections, &request).expect_err("nothing can prepare it");
    assert!(refusal.contains("cannot leave"), "{refusal}");
    assert!(refusal.contains("requires bun"), "{refusal}");
}

/// A machine whose build volume is unplugged is filtered out of cell work
/// the way a drained one is: another machine takes the work and nothing
/// is recorded against this one. A pin to it is refused with the reason,
/// work that needs no cell is not its volume's to refuse, and a volume
/// that is present refuses nothing.
#[test]
fn an_absent_build_volume_filters_cell_work_and_only_cell_work() {
    use cairn_common::executor_protocol::{BuildVolumeReport, BuildVolumeState};
    let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]);
    local.colocated = true;
    let report = |state| BuildVolumeReport {
        root: "/Volumes/Builds/cairn".into(),
        state,
        since_unix_ms: NOW,
        suspended_cells: Vec::new(),
        readopted_cells: 0,
    };
    Arc::make_mut(&mut local.health).build_volume = Some(report(BuildVolumeState::Absent {
        reason: "not mounted".into(),
    }));
    let request = spillable_request();
    let absent = Some(PlacementRejectionReason::BuildVolumeAbsent {
        path: "/Volumes/Builds/cairn".into(),
    });
    assert_eq!(
        candidate_rejection(&local, PlacementScope::of(&request), NOW),
        absent
    );
    let mut pinned = request.clone();
    pinned.pinned_executor_id = Some(COLOCATED_EXECUTOR_ID.into());
    assert_eq!(
        candidate_rejection(&local, PlacementScope::of(&pinned), NOW),
        absent
    );
    let mut live = request.clone();
    live.repository = live_checkout();
    assert_ne!(
        candidate_rejection(&local, PlacementScope::of(&live), NOW),
        absent
    );

    let (remote_id, mut remote) = fleet_entry("bbs", "linux", 0, &[]);
    measured(&mut remote, 0.1, 8_000_000_000, 8_000_000_000);
    let connections = HashMap::from([(local_id.clone(), local.clone()), (remote_id, remote)]);
    let draft = place(&connections, &request).expect("another machine takes it");
    let (selected, _) = draft.selected.expect("placed");
    assert_eq!(selected.executor_id, "bbs");
    assert!(draft.rejected.iter().any(|rejection| matches!(
        rejection.reason,
        PlacementRejectionReason::BuildVolumeAbsent { .. }
    )));

    Arc::make_mut(&mut local.health).build_volume =
        Some(report(BuildVolumeState::Present { mount_point: None }));
    assert_ne!(
        candidate_rejection(&local, PlacementScope::of(&request), NOW),
        absent
    );
}

/// Rust work is refused where the attach-time prerequisite verdict reads
/// MISSING, scoped to projects that declare the script it came from; work
/// that needs no Rust build keeps flowing there.
#[test]
fn a_missing_prerequisite_verdict_refuses_rust_work_for_projects_that_declare_the_script() {
    use cairn_common::executor_protocol::{BuildPrerequisiteCheck, BuildPrerequisiteStatus};
    let (_, mut bbs) = fleet_entry("bbs", "linux", 0, &[]);
    advertising(&mut bbs, &["bun", "rust"]);
    Arc::make_mut(&mut bbs.advertisement)
        .capabilities
        .build_prerequisites = Some(prerequisites(BuildPrerequisiteStatus::Missing));
    let rust_lane = |scripts: &[&str], toolchains: &[&str], setup: &[&str]| {
        let mut request = spillable_request();
        request.executor = (!toolchains.is_empty()).then(|| ExecutorSelector {
            required_toolchains: toolchains.iter().map(|t| t.to_string()).collect(),
            ..ExecutorSelector::default()
        });
        request.project_requirements = project_needs(setup, scripts).into();
        request
    };
    let verdict = |entry: &ExecutorConnectionState, request: &CellRequest| {
        candidate_rejection(entry, PlacementScope::of(request), NOW)
    };

    let refused = verdict(
        &bbs,
        &rust_lane(&[LINUX_PREREQUISITES], &["bun", "rust"], &[]),
    )
    .expect("rust work for a declaring project is refused");
    let PlacementRejectionReason::BuildPrerequisitesMissing { script, .. } = &refused else {
        panic!("unexpected rejection {refused:?}");
    };
    assert_eq!(script, LINUX_PREREQUISITES);
    let described = refused.describe();
    assert!(described.contains("cmake libclang-dev"), "{described}");
    assert!(described.contains("Provision bbs once with"), "{described}");

    // A bun lane is not rust work.
    assert_eq!(
        verdict(&bbs, &rust_lane(&[LINUX_PREREQUISITES], &["bun"], &[])),
        None
    );
    // A project that does not declare the script is described by nothing
    // the probe measured.
    assert_eq!(verdict(&bbs, &rust_lane(&[], &["bun", "rust"], &[])), None);
    // Rust required by the project's setup, with no selector at all.
    assert!(matches!(
        verdict(&bbs, &rust_lane(&[LINUX_PREREQUISITES], &[], &["rust"])),
        Some(PlacementRejectionReason::BuildPrerequisitesMissing { .. })
    ));
    // Unknown is not missing.
    for unknown in [
        Some(prerequisites(BuildPrerequisiteStatus::Unchecked)),
        None,
    ] {
        Arc::make_mut(&mut bbs.advertisement)
            .capabilities
            .build_prerequisites = unknown;
        assert_eq!(
            verdict(
                &bbs,
                &rust_lane(&[LINUX_PREREQUISITES], &["bun", "rust"], &[])
            ),
            None
        );
    }
    // A MISSING from a probe whose script died on the shell's own error
    // about its staged copy, as executors before the staging fix reported
    // it, is not a verdict about the machine.
    Arc::make_mut(&mut bbs.advertisement).capabilities.build_prerequisites = Some(BuildPrerequisiteCheck {
        detail: "/var/folders/k1/T/cairn-prerequisite-b440083b/setup-self-hosted-runner-linux.sh: line 26: /var/folders/k1/T/cairn-prerequisite-b440083b/lib/runner-isolation.sh: No such file or directory".into(),
        ..prerequisites(BuildPrerequisiteStatus::Missing)
    });
    assert_eq!(
        verdict(
            &bbs,
            &rust_lane(&[LINUX_PREREQUISITES], &["bun", "rust"], &[])
        ),
        None
    );
    // A re-attach whose verdict reads satisfied lifts it.
    Arc::make_mut(&mut bbs.advertisement)
        .capabilities
        .build_prerequisites = Some(prerequisites(BuildPrerequisiteStatus::Satisfied));
    assert_eq!(
        verdict(
            &bbs,
            &rust_lane(&[LINUX_PREREQUISITES], &["bun", "rust"], &[])
        ),
        None
    );
}

const WINDOWS_PREREQUISITES: &str = "scripts/setup-self-hosted-runner-windows.ps1";

/// bgsrv-win as its Windows script now reports it: LLVM for libclang, and
/// no clang-cl where cc looks for one.
fn missing_clang_cl() -> cairn_common::executor_protocol::BuildPrerequisiteCheck {
    cairn_common::executor_protocol::BuildPrerequisiteCheck {
        script: format!("{WINDOWS_PREREQUISITES} -CheckPrerequisites"),
        status: cairn_common::executor_protocol::BuildPrerequisiteStatus::Missing,
        detail: "Windows build prerequisites on BGSRV-WIN are missing clang-cl: aegis, in cairn-core's turso closure, compiles its C with clang-cl on MSVC. Add Visual Studio's C++ Clang Compiler for Windows component with: --add Microsoft.VisualStudio.Component.VC.Llvm.Clang".into(),
    }
}

fn windows_without_clang_cl() -> ExecutorConnectionState {
    let (_, mut win) = fleet_entry("bgsrv-win", "windows", 0, &[]);
    advertising(&mut win, &["bun", "rust"]);
    Arc::make_mut(&mut win.advertisement)
        .capabilities
        .build_prerequisites = Some(missing_clang_cl());
    win
}

/// The specimen (cairn://posts/524): an unpinned
/// `bun run test:rust -p cairn-core` names no toolchain and cairn's setup is
/// `bun install`, so only the command says it builds Rust. It is refused,
/// naming what the machine lacks; work that builds no Rust still lands.
#[test]
fn a_cargo_command_is_rust_work_without_a_rust_selector() {
    let win = windows_without_clang_cl();
    let batch = |command: &str| {
        let mut request = spillable_request();
        request.executor = None;
        request.command = command.into();
        request.command_class = CellCommandClass::classify(command);
        request.project_requirements = project_needs(&["bun"], &[WINDOWS_PREREQUISITES]).into();
        request
    };
    let verdict =
        |request: &CellRequest| candidate_rejection(&win, PlacementScope::of(request), NOW);

    for rust in [
        "bun run test:rust -p cairn-core",
        "bun run check:rust",
        "cargo check -p cairn-exec",
    ] {
        let refused = verdict(&batch(rust))
            .unwrap_or_else(|| panic!("{rust} was offered a machine that cannot build it"));
        assert!(
            matches!(
                refused,
                PlacementRejectionReason::BuildPrerequisitesMissing { .. }
            ),
            "{rust}: {refused:?}"
        );
        let described = refused.describe();
        assert!(described.contains("missing clang-cl"), "{described}");
        assert!(
            described.contains("Microsoft.VisualStudio.Component.VC.Llvm.Clang"),
            "{described}"
        );
    }
    for other in [
        "bunx --bun tsc --noEmit",
        "bun run test:frontend",
        "bunx --bun vite build",
    ] {
        assert!(
            !matches!(
                verdict(&batch(other)),
                Some(PlacementRejectionReason::BuildPrerequisitesMissing { .. })
            ),
            "{other}"
        );
    }
}

/// A job's execution environment is where its terminals and unpinned
/// batches run, so it is placed as Rust work: a terminal that lands on a
/// machine that cannot build the project takes every later build with it.
/// Residencies that are not an agent's working tree are not.
#[test]
fn a_jobs_execution_environment_is_rust_work_and_other_residencies_are_not() {
    let mut win = windows_without_clang_cl();
    let acquisition = |holder: ResidencyHolder| {
        let mut request = residency_placement_request(&fleet_residency_request(holder));
        request.project_requirements = project_needs(&["bun"], &[WINDOWS_PREREQUISITES]).into();
        request
    };
    let gated = |entry: &ExecutorConnectionState, request: &CellRequest| {
        matches!(
            candidate_rejection(entry, PlacementScope::of(request), NOW),
            Some(PlacementRejectionReason::BuildPrerequisitesMissing { .. })
        )
    };

    let home = acquisition(ResidencyHolder::Job {
        job_id: "job-1".into(),
    });
    assert!(PlacementScope::of(&home).compiles_rust);
    assert!(gated(&win, &home));

    for holder in [
        ResidencyHolder::Service {
            service_id: "svc".into(),
        },
        ResidencyHolder::Workflow {
            run_id: "run".into(),
        },
        ResidencyHolder::ProjectTerminals {
            project_id: "p".into(),
        },
    ] {
        let other = acquisition(holder.clone());
        assert!(!PlacementScope::of(&other).compiles_rust, "{holder:?}");
        assert!(!gated(&win, &other), "{holder:?}");
    }
    // A request that only carries a job's storage key as its command is not
    // an acquisition.
    let mut lookalike = spillable_request();
    lookalike.command = "job:job-1".into();
    assert!(!PlacementScope::of(&lookalike).compiles_rust);

    // The verdict a re-attach brings once clang-cl is installed lets the
    // environment in.
    Arc::make_mut(&mut win.advertisement)
        .capabilities
        .build_prerequisites = Some(cairn_common::executor_protocol::BuildPrerequisiteCheck {
        status: cairn_common::executor_protocol::BuildPrerequisiteStatus::Satisfied,
        ..missing_clang_cl()
    });
    assert!(!gated(&win, &home));
}

/// The executors page shows what placement enforces, per project.
#[test]
fn the_executors_page_reports_project_eligibility_as_placement_decides_it() {
    let pool = Fleet::default();
    let (id, mut entry) = fleet_entry("bgsrv-ub2", "linux", 0, &[]);
    advertising(&mut entry, &[]);
    Arc::make_mut(&mut entry.advertisement)
        .capabilities
        .projects_served
        .clear();
    pool.lock_connections().insert(id, entry);
    pool.known_project_requirements.lock().unwrap().extend([
        ("p".to_string(), project_needs(&["bun"], &[])),
        ("q".to_string(), project_needs(&[], &[])),
    ]);
    let inspection = pool.inspect_executors(NOW).remove(0);
    assert_eq!(inspection.project_eligibility.len(), 2);
    assert!(matches!(
        inspection.project_eligibility[0].refusals.as_slice(),
        [PlacementRejectionReason::PreparationToolchainMissing { .. }]
    ));
    assert!(inspection.project_eligibility[1].refusals.is_empty());
}

/// The new wire shapes: the outcome variant round-trips, and a request
/// from a peer that predates `projectRequirements` decodes as requiring
/// nothing (and an empty one is not sent).
#[test]
fn preparation_wire_shapes_round_trip_and_default() {
    let reason = CellUnavailableReason::PreparationCommandNotFound {
        command: "bun install".into(),
        program: "bun".into(),
        exit_code: 127,
    };
    let encoded = serde_json::to_string(&reason).unwrap();
    assert_eq!(
        serde_json::from_str::<CellUnavailableReason>(&encoded).unwrap(),
        reason
    );
    let request = spillable_request();
    let encoded = serde_json::to_value(&request).unwrap();
    assert!(encoded.get("projectRequirements").is_none());
    let decoded: CellRequest = serde_json::from_value(encoded).unwrap();
    assert!(decoded.project_requirements.is_empty());
    let mut needing = request;
    needing.project_requirements = project_needs(&["bun"], &[LINUX_PREREQUISITES]).into();
    let decoded: CellRequest =
        serde_json::from_value(serde_json::to_value(&needing).unwrap()).unwrap();
    assert_eq!(decoded.project_requirements, needing.project_requirements);
}

/// A host toolchain missing from the machine excludes it for that project
/// and setup only, and the result names the machine and says whose fault
/// it is. A live checkout, a different project, and a changed setup are
/// not refused on it; a plain preparation failure records nothing.
#[test]
fn a_preparation_refusal_excludes_the_machine_for_its_project_and_setup_only() {
    let pool = Fleet::default();
    let (id, mut entry) = fleet_entry("bgsrv-ub2", "linux", 0, &[]);
    // Serves every project, so only the refusal can tell p from q.
    Arc::make_mut(&mut entry.advertisement)
        .capabilities
        .projects_served
        .clear();
    pool.lock_connections().insert(id, entry);
    let outcome = pool.note_preparation_refusal(
        not_found("bun install --frozen-lockfile", "bun"),
        "bgsrv-ub2",
        "p",
        SETUP,
        true,
    );
    let CellOutcome::Unavailable { diagnostic, .. } = &outcome else {
        panic!("{outcome:?}");
    };
    assert!(diagnostic.starts_with("on bgsrv-ub2:"), "{diagnostic}");
    assert!(
        diagnostic.contains("`bun` is not on bgsrv-ub2's PATH"),
        "{diagnostic}"
    );
    assert!(
        diagnostic.contains("not for your branch or the fleet"),
        "{diagnostic}"
    );
    assert!(
        diagnostic.contains("rebuilt on another machine"),
        "{diagnostic}"
    );

    let connections = pool.lock_connections().clone();
    let entry = &connections["bgsrv-ub2"];
    let verdict =
        |request: &CellRequest| candidate_rejection(entry, PlacementScope::of(request), NOW);
    let mut for_p = spillable_request();
    for_p.executor = None;
    for_p.project_requirements = project_needs(&[], &[]).into();
    assert!(matches!(
        verdict(&for_p),
        Some(PlacementRejectionReason::PreparationRefused { ref program, .. }) if program == "bun"
    ));
    let mut for_q = for_p.clone();
    for_q.project_id = "q".into();
    assert_eq!(verdict(&for_q), None);
    let mut live = for_p.clone();
    live.repository = live_checkout();
    *live.project_requirements = Default::default();
    // A remote cannot take a live checkout at all; what matters is that
    // the refusal is never the preparation one.
    assert!(!verdict(&live).is_some_and(|reason| is_preparation_rejection(&reason)));
    let mut changed = for_p.clone();
    changed.project_requirements.setup_identity = "setup-changed".into();
    assert_eq!(verdict(&changed), None);

    let pool = Fleet::default();
    pool.lock_connections()
        .extend([fleet_entry("bbs", "linux", 0, &[])]);
    pool.note_preparation_refusal(
        CellOutcome::Unavailable {
            reason: CellUnavailableReason::Preparation,
            diagnostic: "lockfile had changes, but lockfile is frozen".into(),
        },
        "bbs",
        "p",
        SETUP,
        false,
    );
    assert!(pool.lock_connections()["bbs"]
        .preparation_refusals
        .is_empty());
}

/// Preparation stamps a project's declared and learned capabilities onto
/// every request, a live checkout's included, since a build fetches its
/// dependencies whether or not setup runs.
#[tokio::test]
async fn preparation_stamps_declared_and_learned_capabilities_on_every_checkout() {
    let project = tempfile::tempdir().unwrap();
    let path = project.path().to_str().unwrap();
    let pool = Fleet::default();
    let mut managed = spillable_request();
    pool.stamp_project_requirements(&mut managed, &[], &[], path);
    assert!(managed.project_requirements.capabilities.is_empty());

    pool.stamp_project_requirements(
        &mut managed,
        &[],
        &[ProjectCapability::GithubCredential],
        path,
    );
    assert_eq!(
        managed.project_requirements.capabilities,
        [ProjectCapability::GithubCredential],
        "declared"
    );

    pool.learn_project_requirement(
        &managed.project_id.clone(),
        ProjectCapability::GithubCredential,
        Some("https://github.com/bleugreen/tm-cards".into()),
        "bginf-sp1",
        "96e4e69d",
    )
    .await;
    let mut live = spillable_request();
    live.repository = live_checkout();
    pool.stamp_project_requirements(&mut live, &[], &[], path);
    assert_eq!(
        live.project_requirements.capabilities,
        [ProjectCapability::GithubCredential],
        "learned, on a live checkout"
    );
    pool.stamp_project_requirements(
        &mut managed,
        &[],
        &[ProjectCapability::GithubCredential],
        path,
    );
    assert_eq!(
        managed.project_requirements.capabilities,
        [ProjectCapability::GithubCredential],
        "declared and learned, once"
    );
    assert_eq!(
        github_probe_repositories(&pool.learned_project_requirements(), &[]),
        ["https://github.com/bleugreen/tm-cards"],
        "the learned repository is what the probe checks"
    );
    assert!(
        github_probe_repositories(&pool.learned_project_requirements(), &["other".into()])
            .is_empty(),
        "a machine that does not serve the project probes nothing for it"
    );
}

/// The setup-toolchain gate is about preparation, so it does not apply to
/// a live checkout, which is never prepared.
#[test]
fn a_live_checkout_is_not_held_to_what_setup_needs() {
    let (_, mut bare) = fleet_entry("bgsrv-ub2", "linux", 0, &[]);
    advertising(&mut bare, &[]);
    let mut managed = spillable_request();
    managed.executor = None;
    managed.project_requirements = project_needs(&["bun"], &[]).into();
    assert!(matches!(
        candidate_rejection(&bare, PlacementScope::of(&managed), NOW),
        Some(PlacementRejectionReason::PreparationToolchainMissing { .. })
    ));
    // Preparation stamps no setup requirement on a live checkout.
    let mut live = managed.clone();
    live.repository = live_checkout();
    *live.project_requirements = Default::default();
    assert!(!matches!(
        candidate_rejection(&bare, PlacementScope::of(&live), NOW),
        Some(
            PlacementRejectionReason::PreparationToolchainMissing { .. }
                | PlacementRejectionReason::PreparationRefused { .. }
        )
    ));
    assert!(!runs_project_setup(&live_checkout()));
}

/// A refusal survives a reconnect with identical evidence (a reconnect is
/// not a repair), is dropped by a re-attach that now finds the toolchain,
/// and is cleared outright by `repaired`.
#[tokio::test]
async fn a_refusal_lasts_until_the_machines_evidence_changes_or_it_is_repaired() {
    let pool = Fleet::default();
    let (id, mut entry) = fleet_entry("bgsrv-ub2", "linux", 0, &[]);
    probed(&mut entry, false);
    let reconnect = entry.clone();
    pool.lock_connections().insert(id.clone(), entry);
    assert!(record(&pool, &id));

    let mut same = reconnect.clone();
    assert!(pool.stamp_preparation_refusals(&mut same).is_empty());
    assert!(same.preparation_refusals.contains_key("p"));

    let mut repaired_by_install = reconnect.clone();
    probed(&mut repaired_by_install, true);
    assert_eq!(
        pool.stamp_preparation_refusals(&mut repaired_by_install),
        vec!["p".to_string()]
    );
    assert!(repaired_by_install.preparation_refusals.is_empty());
    assert!(pool.preparation_refusals.lock().unwrap().held[&id].is_empty());

    assert!(record(&pool, &id));
    pool.clear_preparation_refusals(&id).await.unwrap();
    assert!(pool.lock_connections()[&id].preparation_refusals.is_empty());
    let mut after_repair = reconnect;
    pool.stamp_preparation_refusals(&mut after_repair);
    assert!(after_repair.preparation_refusals.is_empty());
}

/// Refusals persist across a runner restart: hydrated from the store and
/// stamped onto a machine that attached before the store was read.
#[tokio::test]
async fn refusals_are_durable_across_a_runner_restart() {
    let db = Arc::new(crate::storage::migrated_test_db("fleet-preparation-refusals.db").await);
    let (id, mut entry) = fleet_entry("bgsrv-ub2", "linux", 0, &[]);
    probed(&mut entry, false);
    let pool = Fleet::default();
    pool.lock_connections().insert(id.clone(), entry.clone());
    pool.attach_preparation_store(db.clone());
    assert!(record(&pool, &id));
    pool.flush_preparation_store().await.unwrap();

    let restarted = Fleet::default();
    restarted.lock_connections().insert(id.clone(), entry);
    restarted.attach_preparation_store(db);
    restarted.flush_preparation_store().await.unwrap();
    let refusal = &restarted.lock_connections()[&id].preparation_refusals["p"];
    assert_eq!(
        (refusal.program.as_str(), refusal.setup_identity.as_str()),
        ("bun", SETUP)
    );
}

/// The specimen's second batch: a job home on a machine that cannot
/// prepare its project is evicted at the next acquisition, which falls
/// through to placement, and the agent is told why.
#[test]
fn a_home_that_cannot_prepare_its_project_is_rebuilt_elsewhere() {
    for predictive in [false, true] {
        let pool = Fleet::default();
        let (id, mut entry) = answering_fleet_entry("bgsrv-ub2", "linux", 0, &[]);
        advertising(&mut entry, if predictive { &[] } else { &["bun"] });
        pool.lock_connections().insert(id.clone(), entry);
        pool.known_project_requirements
            .lock()
            .unwrap()
            .insert("p".into(), project_needs(&["bun"], &[]));
        if !predictive {
            assert!(record(&pool, &id));
        }
        let mut request = fleet_residency_request(ResidencyHolder::Job {
            job_id: "general".into(),
        });
        home_route(&pool, &id, &request);

        assert!(pool
            .resolve_residency_acquire_route(&mut request)
            .unwrap()
            .is_none());
        assert!(pool.residency_routes.lock().unwrap().is_empty());
        let eviction = pool
            .take_environment_eviction(&request.holder)
            .expect("the eviction is reported");
        let EvictionCause::CannotPrepareProject { project_id, detail } = eviction.cause else {
            panic!("{eviction:?}");
        };
        assert_eq!(project_id, "p");
        assert!(
            detail.contains(if predictive {
                "requires bun"
            } else {
                "`bun` is not on"
            }),
            "{detail}"
        );
    }
}

/// A bad branch must not cost a good machine its home: a missing
/// branch-owned script arrives as plain `Preparation` and records nothing.
/// A live checkout's home is never evicted for preparation, since it never
/// prepares, and nothing is evicted before this runner knows the setup.
#[test]
fn a_branch_owned_failure_a_live_checkout_or_an_unknown_setup_keeps_its_home() {
    for case in ["bad-branch", "live", "unknown-setup"] {
        let pool = Fleet::default();
        let (id, mut entry) = answering_fleet_entry("bglab-mac", "linux", 0, &[]);
        advertising(
            &mut entry,
            if case == "bad-branch" { &["bun"] } else { &[] },
        );
        pool.lock_connections().insert(id.clone(), entry);
        if case != "unknown-setup" {
            pool.known_project_requirements
                .lock()
                .unwrap()
                .insert("p".into(), project_needs(&["bun"], &[]));
        }
        match case {
            "bad-branch" => {
                pool.note_preparation_refusal(
                    CellOutcome::Unavailable {
                        reason: CellUnavailableReason::Preparation,
                        diagnostic: "cell preparation command failed: ./scripts/setup.sh\n/bin/sh: 1: ./scripts/setup.sh: not found".into(),
                    },
                    &id,
                    "p",
                    SETUP,
                    true,
                );
            }
            _ => assert!(record(&pool, &id)),
        }
        let mut request = fleet_residency_request(ResidencyHolder::Job {
            job_id: "good-branch".into(),
        });
        if case == "live" {
            request.repository = live_checkout();
        }
        home_route(&pool, &id, &request);
        assert_eq!(
            pool.resolve_residency_acquire_route(&mut request)
                .unwrap()
                .map(|selected| selected.executor_id),
            Some(id.clone()),
            "{case}"
        );
        assert_eq!(pool.residency_routes.lock().unwrap().len(), 1, "{case}");
        assert!(
            pool.take_environment_eviction(&request.holder).is_none(),
            "{case}"
        );
    }
}

/// cairn/5423's specimen: fedora is drained and holds the main cadence
/// job's environment, and bbs is a machine that could take it. `live` is
/// what runs inside the environment, if anything.
fn pool_with_a_drained_home(
    live: Option<cairn_common::executor_protocol::ResidentProcessKind>,
    another_machine: bool,
) -> (
    Fleet,
    ResidencyAcquireRequest,
    mpsc::UnboundedReceiver<ExecutorMessage>,
) {
    let pool = Fleet::default();
    let (sender, inbox) = mpsc::unbounded_channel();
    let (id, mut entry) = answering_fleet_entry("fedora", "linux", 0, &[]);
    entry.sender = sender;
    entry.drained = true;
    let mut cell = materialization_read_cell("fedora-home", "cadence");
    if let Some(kind) = live {
        cell.occupancy.processes.insert(
            "live".into(),
            cairn_common::executor_protocol::ResidentProcess {
                generation: 1,
                kind,
                spec: None,
                status: cairn_common::executor_protocol::ResidentProcessStatus::Starting,
                reservation: None,
                live_usage: None,
                charge: None,
            },
        );
    }
    Arc::make_mut(&mut entry.snapshot).cells = vec![cell];
    pool.lock_connections().insert(id.clone(), entry);
    if another_machine {
        pool.lock_connections()
            .extend([answering_fleet_entry("bbs", "linux", 0, &[])]);
    }
    let request = fleet_residency_request(ResidencyHolder::Job {
        job_id: "cadence".into(),
    });
    home_route(&pool, &id, &request);
    (pool, request, inbox)
}

/// An idle environment on a drained machine is not resident work: its
/// job's next batch is new work. The route is forgotten, the cell is
/// released on fedora, and the acquisition falls through to placement,
/// which passes fedora over for the drain and takes bbs. The agent is told
/// the environment was rebuilt because of the drain.
#[test]
fn an_idle_environment_on_a_drained_machine_is_released_and_acquired_elsewhere() {
    let (pool, mut request, mut inbox) = pool_with_a_drained_home(None, true);

    assert!(pool
        .resolve_residency_acquire_route(&mut request)
        .unwrap()
        .is_none());
    assert!(pool.residency_routes.lock().unwrap().is_empty());
    assert!(matches!(
        inbox.try_recv(),
        Ok(ExecutorMessage::ResidencyRequest {
            operation: ResidencyOperation::Release { .. },
            ..
        })
    ));
    let eviction = pool
        .take_environment_eviction(&request.holder)
        .expect("the move is reported");
    assert_eq!(eviction.executor, "fedora");
    assert_eq!(eviction.cause, EvictionCause::Drained);
    assert!(eviction.lost_processes.is_empty());

    let placement = residency_placement_request(&request);
    let now = unix_time_ms();
    let connections = pool.lock_connections();
    assert_eq!(
        candidate_rejection(&connections["fedora"], PlacementScope::of(&placement), now),
        Some(PlacementRejectionReason::Drained)
    );
    assert_eq!(
        candidate_rejection(&connections["bbs"], PlacementScope::of(&placement), now),
        None
    );
    drop(connections);
    assert!(pool.drained_home_holding(&request.holder).is_none());
}

/// A terminal in the environment is resident work, which a drain leaves
/// alone: the batch goes home, past the drain, and says why.
#[test]
fn a_live_terminal_keeps_its_environment_on_a_drained_machine_and_says_so() {
    let (pool, mut request, mut inbox) = pool_with_a_drained_home(
        Some(cairn_common::executor_protocol::ResidentProcessKind::Terminal { slug: "dev".into() }),
        true,
    );

    assert_eq!(
        pool.resolve_residency_acquire_route(&mut request)
            .unwrap()
            .map(|selected| selected.executor_id),
        Some("fedora".to_string())
    );
    assert_eq!(pool.residency_routes.lock().unwrap().len(), 1);
    assert!(inbox.try_recv().is_err(), "nothing is released");
    assert!(pool.take_environment_eviction(&request.holder).is_none());

    let home = pool
        .drained_home_holding(&request.holder)
        .expect("the batch is told it is on a drained machine");
    assert_eq!(
        home.sentence(),
        "**fedora** is draining, and this batch was placed there because terminal `dev` holds this job's environment there. Once nothing live holds it, the next batch moves to a machine that is not draining."
    );
    // Keeping the resident environment does not admit new work, even when
    // that work is pinned to the machine holding the terminal.
    let mut pinned = residency_placement_request(&request);
    pinned.pinned_executor_id = Some("fedora".into());
    assert_eq!(
        candidate_rejection(
            &pool.lock_connections()["fedora"],
            PlacementScope::of(&pinned),
            unix_time_ms()
        ),
        Some(PlacementRejectionReason::Drained)
    );
}

/// Releasing an environment no other machine could take would only
/// rebuild it on the drained machine and lose its state at every batch.
/// Alone on the fleet, or a live checkout that exists only where it is,
/// it stays, and the batch says why.
#[test]
fn an_environment_nothing_else_can_take_stays_on_a_drained_machine() {
    for case in ["alone", "live-checkout"] {
        let (pool, mut request, mut inbox) = pool_with_a_drained_home(None, case != "alone");
        if case == "live-checkout" {
            request.repository = live_checkout();
            pool.residency_routes.lock().unwrap().clear();
            home_route(&pool, "fedora", &request);
        }
        assert_eq!(
            pool.resolve_residency_acquire_route(&mut request)
                .unwrap()
                .map(|selected| selected.executor_id),
            Some("fedora".to_string()),
            "{case}"
        );
        assert!(inbox.try_recv().is_err(), "{case}");
        assert!(
            pool.take_environment_eviction(&request.holder).is_none(),
            "{case}"
        );
        assert_eq!(
            pool.drained_home_holding(&request.holder)
                .map(|home| home.sentence()),
            Some("**fedora** is draining, and this batch was placed there because no other machine can take this job's environment.".to_string()),
            "{case}"
        );
    }
}

/// What a drain keeps: every resident environment, and by name the ones
/// live work holds, which is what the drain write and the executors page
/// both say.
#[test]
fn drain_residents_count_every_environment_and_name_the_held_ones() {
    let mut snapshot = FleetSnapshot::default();
    let idle = materialization_read_cell("idle", "cadence");
    let mut terminal = materialization_read_cell("terminal", "builder");
    terminal.occupancy.processes.insert(
        "terminal:dev".into(),
        cairn_common::executor_protocol::ResidentProcess {
            generation: 1,
            kind: cairn_common::executor_protocol::ResidentProcessKind::Terminal {
                slug: "dev".into(),
            },
            spec: None,
            status: cairn_common::executor_protocol::ResidentProcessStatus::Starting,
            reservation: None,
            live_usage: None,
            charge: None,
        },
    );
    let mut unowned = materialization_read_cell("unowned", "nobody");
    unowned.residency = None;
    snapshot.cells = vec![idle, terminal, unowned];

    let residents = DrainResidents::of(&snapshot);
    assert_eq!(residents.environments, 2);
    assert_eq!(
        residents.held,
        vec![HeldEnvironment {
            owner: "job builder".into(),
            held_by: vec!["terminal `dev`".into()],
        }]
    );
    assert_eq!(
        residents.headline(),
        "refusing new work; 2 resident environments, 1 held by live processes"
    );
    assert_eq!(
        residents.account(),
        "It keeps 2 resident environments, 1 held by live processes and left alone until that work stops (job builder, by terminal `dev`); the idle one is released at its job's next batch and acquired on a machine that is not draining."
    );
    assert_eq!(
        DrainResidents::default().account(),
        "Nothing is resident there."
    );
}

/// A refusal describes the setup it failed under. Once the project runs a
/// different setup it is forgotten on every machine, not left standing.
#[test]
fn a_changed_setup_forgets_the_refusals_recorded_under_the_old_one() {
    let pool = Fleet::default();
    pool.lock_connections().extend([
        fleet_entry("bgsrv-ub2", "linux", 0, &[]),
        fleet_entry("bbs", "linux", 0, &[]),
    ]);
    for id in ["bgsrv-ub2", "bbs"] {
        assert!(record(&pool, id));
    }
    pool.forget_superseded_setup_refusals("p", SETUP);
    assert_eq!(
        pool.preparation_refusals.lock().unwrap().held["bbs"].len(),
        1
    );

    pool.forget_superseded_setup_refusals("p", "setup-changed");
    for id in ["bgsrv-ub2", "bbs"] {
        assert!(pool.preparation_refusals.lock().unwrap().held[id].is_empty());
        assert!(pool.lock_connections()[id].preparation_refusals.is_empty());
    }
}

/// The store is written in the order memory changed. A recording whose
/// write is still queued cannot outlive a `repaired` that follows it, a
/// restart cannot bring it back, and startup hydration cannot restore a
/// row that a repair cleared while the store was still being read.
#[tokio::test]
async fn a_repair_is_never_overtaken_by_an_older_recording_or_by_hydration() {
    let db = Arc::new(crate::storage::migrated_test_db("preparation-ordering.db").await);
    let (id, mut entry) = fleet_entry("bgsrv-ub2", "linux", 0, &[]);
    probed(&mut entry, false);

    // Record, then repair, with the recording's write still queued: no
    // await separates them, so the writer has not run.
    let pool = Fleet::default();
    pool.lock_connections().insert(id.clone(), entry.clone());
    pool.attach_preparation_store(db.clone());
    assert!(record(&pool, &id));
    pool.clear_preparation_refusals(&id).await.unwrap();
    assert!(preparation::load_all(&db).await.unwrap().is_empty());

    let restarted = Fleet::default();
    restarted
        .lock_connections()
        .insert(id.clone(), entry.clone());
    restarted.attach_preparation_store(db.clone());
    restarted.flush_preparation_store().await.unwrap();
    assert!(restarted.lock_connections()[&id]
        .preparation_refusals
        .is_empty());

    // An old row on disk, repaired before startup hydration reads it.
    preparation::record(
        &db,
        &id,
        &preparation::PreparationRefusal {
            project_id: "p".into(),
            environment_identity: preparation_environment_identity(&entry),
            setup_identity: SETUP.into(),
            command: "bun install".into(),
            program: "bun".into(),
            diagnostic: "bun: not found".into(),
            recorded_at_unix_ms: NOW,
        },
    )
    .await
    .unwrap();
    let racing = Fleet::default();
    racing.lock_connections().insert(id.clone(), entry);
    racing.attach_preparation_store(db.clone());
    racing.clear_preparation_refusals(&id).await.unwrap();
    assert!(racing
        .preparation_refusals
        .lock()
        .unwrap()
        .held
        .get(&id)
        .is_none_or(HashMap::is_empty));
    assert!(racing.lock_connections()[&id]
        .preparation_refusals
        .is_empty());
    assert!(preparation::load_all(&db).await.unwrap().is_empty());
}

/// The held refusals and each connection's copy change under one lock
/// order, so a recording racing a repair can never leave the connection
/// — which placement reads — holding a refusal the repair cleared.
#[test]
fn a_recording_racing_a_repair_never_leaves_a_stale_connection_copy() {
    for _ in 0..200 {
        let pool = Fleet::default();
        pool.lock_connections()
            .extend([fleet_entry("bgsrv-ub2", "linux", 0, &[])]);
        let start = Arc::new(std::sync::Barrier::new(2));
        let recorder = {
            let (pool, start) = (pool.clone(), start.clone());
            std::thread::spawn(move || {
                start.wait();
                record(&pool, "bgsrv-ub2");
            })
        };
        let repairer = {
            let (pool, start) = (pool.clone(), start.clone());
            std::thread::spawn(move || {
                start.wait();
                tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap()
                    .block_on(pool.clear_preparation_refusals("bgsrv-ub2"))
                    .unwrap();
            })
        };
        recorder.join().unwrap();
        repairer.join().unwrap();
        let held = pool
            .preparation_refusals
            .lock()
            .unwrap()
            .held
            .get("bgsrv-ub2")
            .cloned()
            .unwrap_or_default();
        assert_eq!(
            pool.lock_connections()["bgsrv-ub2"].preparation_refusals,
            held
        );
    }
}

/// A repair that could not be made durable is reported as failed: a
/// delete that failed, and a store whose writer has stopped, are not
/// "repaired" — a restart would bring the refusal back.
#[tokio::test]
async fn a_repair_that_is_not_durable_fails() {
    let db = Arc::new(crate::storage::migrated_test_db("preparation-repair-failure.db").await);
    let pool = Fleet::default();
    pool.lock_connections()
        .extend([fleet_entry("bgsrv-ub2", "linux", 0, &[])]);
    pool.attach_preparation_store(db.clone());
    pool.flush_preparation_store().await.unwrap();
    db.execute_batch("DROP TABLE executor_preparation_refusals")
        .await
        .unwrap();
    assert!(record(&pool, "bgsrv-ub2"));
    let failed = pool
        .clear_preparation_refusals("bgsrv-ub2")
        .await
        .unwrap_err();
    assert!(failed.contains("preparation refusal"), "{failed}");

    let stopped = Fleet::default();
    stopped
        .lock_connections()
        .extend([fleet_entry("bgsrv-ub2", "linux", 0, &[])]);
    let (writer, writes) = mpsc::unbounded_channel();
    drop(writes);
    stopped.preparation_refusals.lock().unwrap().attach(writer);
    let failed = stopped
        .clear_preparation_refusals("bgsrv-ub2")
        .await
        .unwrap_err();
    assert!(failed.contains("stopped"), "{failed}");
}

/// The ordering the cached setup alone gets wrong: a refusal recorded
/// under the old setup, the project's canonical setup changed, and the
/// next acquisition. It must judge the home by the setup the project runs
/// now, so the existing home is kept rather than evicted.
#[tokio::test]
async fn an_acquisition_judges_its_home_by_the_current_canonical_setup() {
    let temp = tempfile::tempdir().unwrap();
    let orch = test_orchestrator(temp.path()).await;
    let repository_path = temp.path().join("repository");
    std::fs::create_dir_all(&repository_path).unwrap();
    orch.db
        .local
        .execute_script(format!(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) \
                 VALUES ('p', 'default', 'Project', 'p', '{}', 1, 1);",
            repository_path.to_string_lossy()
        ))
        .await
        .unwrap();
    let holder = ResidencyHolder::Job {
        job_id: "setup-changed".into(),
    };
    let repository = RepositoryLocator::ColocatedPath {
        project_id: "p".into(),
        repository_id: "p".into(),
        absolute_path: repository_path.to_string_lossy().into_owned(),
    };
    let (sender, mut executor) = mpsc::unbounded_channel();
    let generation = orch.fleet.attach_executor(sender);
    orch.fleet.residency_routes.lock().unwrap().insert(
        (COLOCATED_EXECUTOR_ID.into(), holder.storage_key()),
        ResidencyRoute {
            holder: holder.clone(),
            repository: repository.clone(),
            executor_id: COLOCATED_EXECUTOR_ID.into(),
            pending: false,
        },
    );
    let mut cell = materialization_read_cell("resident-cell", "setup-changed");
    cell.project_id = "p".into();
    cell.residency.as_mut().unwrap().repository = repository.clone();
    assert!(orch.fleet.set_executor_snapshot(
        COLOCATED_EXECUTOR_ID,
        generation,
        FleetSnapshot {
            cells: vec![cell.clone()],
            ..FleetSnapshot::default()
        },
        ExecutorSubstrateReport::default(),
    ));
    // What the runner believed before the canonical setup changed: the
    // home's machine failed the old setup, and the cache still says so.
    orch.fleet
        .known_project_requirements
        .lock()
        .unwrap()
        .insert(
            "p".into(),
            ProjectPlacementRequirements {
                setup_identity: "old-setup".into(),
                ..ProjectPlacementRequirements::default()
            },
        );
    assert!(orch.fleet.record_preparation_refusal(
        COLOCATED_EXECUTOR_ID,
        "p",
        "old-setup",
        "bun install",
        "bun",
        "bun: not found"
    ));

    let answering_fleet = orch.fleet.clone();
    let answer = tokio::spawn(async move {
        let correlation_id = loop {
            match executor.recv().await {
                Some(ExecutorMessage::DrainModeRequest { .. }) => continue,
                Some(ExecutorMessage::ResidencyRequest {
                    correlation_id,
                    operation: ResidencyOperation::Acquire { .. },
                    ..
                }) => break correlation_id,
                other => panic!("the existing home must be reattached, got {other:?}"),
            }
        };
        answering_fleet.handle_executor_message(
            COLOCATED_EXECUTOR_ID,
            generation,
            ExecutorMessage::ResidencyResponse {
                correlation_id,
                result: ResidencyResult::State { cell },
            },
        );
    });
    let mut request = fleet_residency_request(holder.clone());
    request.repository = repository;
    request.wait_horizon_unix_ms = unix_time_ms().saturating_add(5_000);
    let result = orch
        .fleet
        .operate_residency(&orch, ResidencyOperation::Acquire { request })
        .await;
    answer.await.unwrap();
    assert!(
        matches!(result, ResidencyResult::State { .. }),
        "{result:?}"
    );
    assert!(orch.fleet.take_environment_eviction(&holder).is_none());
    assert!(orch.fleet.lock_connections()[COLOCATED_EXECUTOR_ID]
        .preparation_refusals
        .is_empty());
}

#[tokio::test]
async fn residency_queue_duplicate_horizon_names_live_entry_and_state() {
    residency_queue_scenario(async {
            let temp = tempfile::tempdir().unwrap();
            let orch = test_orchestrator(temp.path()).await;
            let (generation, mut remote) = residency_queue_attach(&orch.fleet, "remote");
            let request = residency_queue_request("horizon");
            let (winner, ()) = tokio::join!(
                orch.fleet.operate_residency(&orch, ResidencyOperation::Acquire { request: request.clone() }),
                async {
                    let entry = residency_queue_offer(&mut remote).await;
                    let mut duplicate = request.clone();
                    duplicate.wait_horizon_unix_ms = unix_time_ms() + 30;
                    let result = orch.fleet.operate_residency(&orch, ResidencyOperation::Acquire { request: duplicate }).await;
                    assert!(matches!(result, ResidencyResult::Failed { ref diagnostic, .. } if diagnostic.contains(&entry) && diagnostic.contains("Offered") && diagnostic.contains("wait horizon")), "{result:?}");
                    assert_eq!(orch.fleet.queue.len(), 1, "duplicate expiry must not remove winner");
                    orch.fleet.handle_executor_message("remote", generation, ExecutorMessage::Claim { entry_id: entry.clone() });
                    let (correlation, granted) = residency_queue_grant(&mut remote, Some(&entry)).await;
                    residency_queue_reply(&orch.fleet, "remote", generation, correlation, &granted);
                }
            );
            assert!(matches!(winner, ResidencyResult::State { .. }), "{winner:?}");
        }).await;
}

/// A grant speaks only for the link it was answered on. With that link
/// gone, the refusal names the machine and that it stopped answering; on
/// the machine's next link, before and after that link's opening snapshot,
/// the refusal names what the runner then knows.
#[tokio::test]
async fn a_grant_on_a_retired_link_is_refused_naming_the_routing_case() {
    residency_queue_scenario(async {
        let temp = tempfile::tempdir().unwrap();
        let orch = test_orchestrator(temp.path()).await;
        let (generation, mut remote) = residency_queue_attach(&orch.fleet, "remote");
        let cell = residency_grant_on_remote(&orch, &mut remote, generation, "retired").await;
        let key = (
            "remote".to_string(),
            cell.residency.as_ref().unwrap().holder.storage_key(),
        );
        assert!(orch.fleet.disconnect_advertised_executor_with_origin(
            "remote",
            generation,
            ExecutorDisconnectOrigin::RunnerInitiated
        ));
        let detached = residency_stop_refusal(&orch, &cell).await;
        assert!(
            detached
                .starts_with("remote holds this execution environment and has not answered since ")
                && detached.ends_with("; disconnected)"),
            "{detached}"
        );

        let (next_generation, _next) = residency_queue_attach(&orch.fleet, "remote");
        assert_ne!(next_generation, generation);
        assert_eq!(
            residency_stop_refusal(&orch, &cell).await,
            "remote's latest report no longer carries this execution environment"
        );
        orch.fleet.handle_executor_message(
            "remote",
            next_generation,
            ExecutorMessage::SnapshotUpdated {
                snapshot: FleetSnapshot::default(),
                health: ExecutorSubstrateReport::default(),
            },
        );
        assert!(!orch.fleet.granted_cells.lock().unwrap().contains_key(&key));
        assert!(!orch
            .fleet
            .residency_routes
            .lock()
            .unwrap()
            .contains_key(&key));
        assert_eq!(
            residency_stop_refusal(&orch, &cell).await,
            "no executor holds a route to this execution environment"
        );
    })
    .await;
}

/// Object authority names one machine, one request, and one connection
/// generation, and only the runner closes it. Opening it at the claim left
/// it open whenever the runner then failed to dispatch: the machine never
/// received `Granted`, so it had nothing to fail and nothing to report, and
/// its read access to that request's objects survived until the connection
/// ended. Authority belongs to the grant, not to the claim.
#[tokio::test]
async fn a_claim_the_runner_never_grants_leaves_no_object_authority_behind() {
    let temp = tempfile::tempdir().unwrap();
    let orch = Arc::new(test_orchestrator(temp.path()).await);
    let (sender, mut remote) = mpsc::unbounded_channel();
    let (_, advertised) = fleet_entry("remote", "macos", 0, &[]);
    let generation = orch
        .fleet
        .attach_advertised_executor(
            Arc::unwrap_or_clone(advertised.advertisement),
            Default::default(),
            sender,
            false,
            None,
            None,
        )
        .expect("attach an enrolled machine");
    orch.fleet.announce_room_for_test("remote", generation);

    let mut request = provenance_request();
    request.repository = RepositoryLocator::ScratchOnly {
        owner_id: request.project_id.clone(),
    };
    // Placement will not choose a machine it cannot see, and a fixture
    // machine posts no measurements. The pin settles placement so this test
    // is about what happens after the claim rather than about ranking.
    request.pinned_executor_id = Some("remote".into());
    // A batch naming a runner callback context this runner does not hold is
    // the smallest pre-grant failure there is: binding that context is the
    // last fallible step between the claim and the grant.
    let batch = ProcessBatch {
        files: Vec::new(),
        runtime_packages: Vec::new(),
        sequential: true,
        stop_on_error: true,
        sandbox_mode: ProcessSandboxMode::Unconfined,
        timeout_bases: Vec::new(),
        items: vec![cache_batch_item(Vec::new())],
        runner_context_id: Some("a context the runner does not hold".into()),
        execution_residency: None,
        containment: None,
        node_scratch: None,
    };

    let submitting = {
        let orch = orch.clone();
        tokio::spawn(async move {
            orch.fleet
                .submit_execution(&orch, request, Some(batch))
                .await
        })
    };
    // Answering here rather than in the spawned task keeps the receiver in
    // hand afterwards, so what the machine did NOT hear is checkable, and
    // bounds every wait: a runner that never offers is a failure this test
    // reports rather than a hang.
    let (offered, summary) = loop {
        match tokio::time::timeout(std::time::Duration::from_secs(30), remote.recv()).await {
            Ok(Some(ExecutorMessage::DrainModeRequest { .. })) => continue,
            Ok(Some(ExecutorMessage::Offer { entry_id, summary })) => break (entry_id, summary),
            Err(_) => {
                let outcome = submitting.await.unwrap();
                panic!(
                    "the runner never offered this work to the one machine attached: {outcome:?}"
                )
            }
            other => panic!("the chosen machine hears an offer first, got {other:?}"),
        }
    };
    let (_machine_home, machine) = claiming_machine();
    machine_claims(&machine, &offered, &summary).await;
    orch.fleet.handle_executor_message(
        "remote",
        generation,
        ExecutorMessage::Claim {
            entry_id: offered.clone(),
        },
    );
    let outcome = submitting.await.unwrap();

    assert!(
        matches!(
            outcome,
            CellOutcome::Unavailable { ref diagnostic, .. }
                if diagnostic.contains("unknown runner callback context")
        ),
        "the dispatch fails after the claim, got {outcome:?}"
    );
    // The machine is told the grant is never coming, and nothing else: no
    // grant, and nothing that would leave its ticket standing.
    match remote.try_recv() {
        Ok(frame @ ExecutorMessage::Lost { .. }) => {
            assert!(matches!(&frame, ExecutorMessage::Lost { entry_id } if *entry_id == offered));
            machine_hears(&machine, frame).await;
        }
        other => panic!("an abandoned claim is withdrawn with Lost, got {other:?}"),
    }
    assert!(
        remote.try_recv().is_err(),
        "a machine whose claim the runner could not dispatch never receives a grant"
    );
    assert_machine_released(&machine, &summary).await;
    assert!(
        !orch.object_plane.authorizes(
            &ObjectTransferCoordinate {
                repository: RepositoryLocator::ScratchOnly {
                    owner_id: "p".into(),
                }
                .identity(),
                request_id: "provenance".into(),
                attempt_id: "attempt".into(),
                executor_id: "remote".into(),
                connection_generation: generation,
            },
            "base",
        ),
        "a machine that never received a grant holds no authority over that request's objects"
    );
}

/// A withdrawal standing from an untargeted acquisition steers later ones. It
/// must not stop a caller who then names that machine from reaching it.
#[tokio::test(flavor = "multi_thread")]
async fn a_standing_withdrawal_never_bars_a_machine_the_caller_names() {
    residency_queue_scenario(async {
        let temp = tempfile::tempdir().unwrap();
        let orch = test_orchestrator(temp.path()).await;
        let (generation, mut only) = residency_queue_attach(&orch.fleet, "only");
        let untargeted = residency_queue_untargeted("thread");
        let (refused, ()) = tokio::join!(
            orch.fleet.operate_residency(
                &orch,
                ResidencyOperation::Acquire {
                    request: untargeted.clone()
                }
            ),
            residency_queue_refuses(
                &orch.fleet,
                "only",
                generation,
                &mut only,
                ResidencyFailureKind::Persistence,
                "provision residency checkout: Access is denied",
            )
        );
        assert!(
            matches!(refused, ResidencyResult::Failed { .. }),
            "{refused:?}"
        );
        assert_eq!(
            orch.fleet.withdrawn_home_executors(&untargeted.holder),
            vec!["only".to_string()],
            "an untargeted request is free to go elsewhere, so it is withdrawn"
        );

        let mut named = untargeted.clone();
        named.executor = Some(ExecutorSelector {
            name: Some("only".into()),
            ..Default::default()
        });
        let (placed, ()) = tokio::join!(
            orch.fleet.operate_residency(
                &orch,
                ResidencyOperation::Acquire {
                    request: named.clone()
                }
            ),
            residency_queue_accepts(&orch.fleet, "only", generation, &mut only)
        );
        assert!(
            matches!(placed, ResidencyResult::State { .. }),
            "the machine the caller named is asked: {placed:?}"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_thread_home_lands_off_platform_when_nothing_else_can_take_it() {
    residency_queue_scenario(async {
        let temp = tempfile::tempdir().unwrap();
        let orch = test_orchestrator(temp.path()).await;
        let (generation, mut only) =
            residency_queue_attach_on(&orch.fleet, "only", another_platform());
        let request = residency_queue_untargeted("thread");
        let prices = HomePrices {
            home_affinity: Some(orch.fleet.thread_home_affinity(&request.holder)),
            scratch_sync: None,
        };
        let (placed, ()) = tokio::join!(
            orch.fleet
                .acquire_priced_home(&orch, request.clone(), prices),
            residency_queue_accepts(&orch.fleet, "only", generation, &mut only)
        );
        assert!(
            matches!(placed, ResidencyResult::State { .. }),
            "priced, not pinned: {placed:?}"
        );
        assert_eq!(
            orch.fleet.residency_route_executor(&request.holder),
            Some("only".to_string())
        );
    })
    .await;
}

/// The specimen ordering: the executor's answer to a grant reaches the
/// runner before any snapshot showing the cell. An operation sent straight
/// after the acquire routes to the granting link by the grant itself, and
/// the fence a holder-only caller reads comes from the grant too.
#[tokio::test]
async fn an_operation_right_after_its_grant_routes_to_the_granting_link() {
    residency_queue_scenario(async {
        let temp = tempfile::tempdir().unwrap();
        let orch = test_orchestrator(temp.path()).await;
        let (generation, mut remote) = residency_queue_attach(&orch.fleet, "remote");
        let cell = residency_grant_on_remote(&orch, &mut remote, generation, "granted").await;
        let holder = cell.residency.as_ref().unwrap().holder.clone();
        assert!(
            orch.fleet.lock_connections()["remote"]
                .snapshot
                .cells
                .is_empty(),
            "no snapshot carries the granted cell"
        );
        assert_eq!(
            orch.fleet.residency_fence(&holder),
            Some(residency_fence_of(&cell))
        );
        let result =
            residency_stop_routed_to(&orch, &mut remote, "remote", generation, &cell).await;
        assert!(
            matches!(result, ResidencyResult::State { .. }),
            "{result:?}"
        );
    })
    .await;
}

/// A freshly attached machine on another platform has never been selected,
/// which sorts it first on every tie, and its id sorts first too.
#[tokio::test(flavor = "multi_thread")]
async fn a_thread_home_prefers_the_runners_platform_over_a_fresh_machine() {
    residency_queue_scenario(async {
        let temp = tempfile::tempdir().unwrap();
        let orch = test_orchestrator(temp.path()).await;
        let (_, mut fresh) = residency_queue_attach_on(&orch.fleet, "a-fresh", another_platform());
        let (runner_generation, mut runner) =
            residency_queue_attach_on(&orch.fleet, "b-runners", std::env::consts::OS);
        let request = residency_queue_untargeted("thread");
        let prices = HomePrices {
            home_affinity: Some(orch.fleet.thread_home_affinity(&request.holder)),
            scratch_sync: None,
        };
        let (placed, ()) = tokio::join!(
            orch.fleet
                .acquire_priced_home(&orch, request.clone(), prices),
            residency_queue_accepts(&orch.fleet, "b-runners", runner_generation, &mut runner)
        );
        assert!(
            matches!(placed, ResidencyResult::State { .. }),
            "{placed:?}"
        );
        assert_never_offered(&mut fresh, "the machine that just attached");
        assert_eq!(
            orch.fleet
                .thread_home_affinity(&request.holder)
                .previous_executor_id,
            Some("b-runners".to_string()),
            "the machine that answered is the thread's previous home from here on"
        );
    })
    .await;
}

/// A platform no test machine that means to lose is ever on by accident.
fn another_platform() -> &'static str {
    match std::env::consts::OS {
        "windows" => "linux",
        _ => "windows",
    }
}

async fn residency_stop_refusal(orch: &Orchestrator, cell: &PersistentCellState) -> String {
    let result = orch
        .fleet
        .operate_residency(
            orch,
            ResidencyOperation::StopProcess {
                fence: residency_fence_of(cell),
                process_key: "svc".into(),
            },
        )
        .await;
    match result {
        ResidencyResult::Failed {
            kind: ResidencyFailureKind::Unavailable,
            diagnostic,
            ..
        } => diagnostic,
        other => panic!("an unroutable operation is refused Unavailable: {other:?}"),
    }
}
