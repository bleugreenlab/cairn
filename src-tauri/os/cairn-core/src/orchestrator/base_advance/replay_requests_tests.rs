//! Real-store explicit replay tests, on the preparation harness: a jj store, a
//! migrated database, and an orchestrator pointed at both.

use super::*;
use crate::orchestrator::base_advance::preparation::tests::{
    git, harness_or_skip, Harness, BRANCH, JOB, MAIN_LINE,
};
use crate::orchestrator::conflict_session::load_active_session;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

type Hook = Box<dyn FnMut(u32) + Send>;

fn hooks() -> &'static Mutex<HashMap<PathBuf, Hook>> {
    static HOOKS: OnceLock<Mutex<HashMap<PathBuf, Hook>>> = OnceLock::new();
    HOOKS.get_or_init(Default::default)
}

/// Called after the carry is planned, before the preparer captures.
pub(super) fn after_carry_plan(store: &Path, attempt: u32) {
    let mut hook = hooks().lock().unwrap().remove(store);
    if let Some(hook) = hook.as_mut() {
        hook(attempt);
    }
    if let Some(hook) = hook {
        hooks().lock().unwrap().insert(store.to_path_buf(), hook);
    }
}

struct HookGuard(PathBuf);

impl Drop for HookGuard {
    fn drop(&mut self) {
        hooks().lock().unwrap().remove(&self.0);
    }
}

fn set_hook(h: &Harness, hook: impl FnMut(u32) + Send + 'static) -> HookGuard {
    hooks()
        .lock()
        .unwrap()
        .insert(h.store.clone(), Box::new(hook));
    HookGuard(h.store.clone())
}

const SIBLING: &str = "agent/dormant";

/// The branch appends X where D1 appends A; the owner's commit records the
/// session, and the owner commits the resolution A-then-X on the old fork.
async fn resolved_conflict(h: &Harness) -> (String, String) {
    h.commit(&h.branch_ws, BRANCH, "shared.txt", "original\nX\n");
    let d1 = h.advance_main("shared.txt", "original\nA\n");
    let refusal = h
        .prepare(PreparationPurpose::OwnerCommit)
        .await
        .unwrap_err();
    assert!(
        matches!(refusal, PreparationRefusal::Conflict(_)),
        "the owner's commit records the conflict: {refusal}"
    );
    assert!(load_active_session(&h.db, BRANCH).await.unwrap().is_some());
    let resolved = h.commit(&h.branch_ws, BRANCH, "shared.txt", "original\nA\nX\n");
    (d1, resolved)
}

fn show(h: &Harness, revision: &str, path: &str) -> String {
    git(&h.repo, &["show", &format!("{revision}:{path}")])
}

async fn sibling_items(h: &Harness) -> i64 {
    h.db.query_text(
        "SELECT CAST(COUNT(*) AS TEXT) FROM jj_reconcile_items WHERE bookmark = ?1",
        (SIBLING,),
    )
    .await
    .unwrap()
    .unwrap()
    .parse()
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn an_explicit_resolved_replay_lands_past_the_bound_session_shortcut_on_its_branch_only() {
    let h = harness_or_skip!("explicit_lands");
    let sibling_ws = h.workspaces.path().join("dormant");
    crate::jj::add_workspace(&h.jj, &h.store, &sibling_ws, SIBLING, "main", None).unwrap();
    h.db.execute_script(format!(
        "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
           VALUES ('job-dormant', 'exec-1', 'dormant', 'issue-1', 'proj-1', 'running', '{SIBLING}', NULL, 2, 2);"
    ))
    .await
    .unwrap();
    let sibling = h.commit(&sibling_ws, SIBLING, "dormant.txt", "dormant\n");
    let (d1, _) = resolved_conflict(&h).await;

    // An ordinary commit resolving the recorded conflict binds to the session
    // and is not carried onto the base.
    let bound = h.prepare(PreparationPurpose::OwnerCommit).await.unwrap();
    assert!(!bound.advanced && bound.session.is_some());

    // The owner's explicit request is never answered by that shortcut.
    let answer = request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, true, None)
        .await
        .unwrap();
    assert!(answer.starts_with("Replayed"), "{answer}");
    assert!(answer.contains("not a green build"), "{answer}");
    let tip = h.tip(BRANCH);
    assert!(h.is_ancestor(&d1, &tip), "the replay carries D1");
    assert_eq!(show(&h, &tip, "shared.txt"), "original\nA\nX");
    assert_eq!(
        git(&h.repo, &["rev-parse", BRANCH]),
        tip,
        "the Git ref is exported"
    );
    assert!(load_active_session(&h.db, BRANCH).await.unwrap().is_none());

    assert_eq!(h.tip(SIBLING), sibling, "a dormant sibling is untouched");
    assert!(!h.is_ancestor(&d1, &sibling));
    assert_eq!(sibling_items(&h).await, 0, "nor ever enumerated");
    assert_eq!(
        h.wakes()
            .await
            .iter()
            // A Steer delivery is stored at the `wake` level; the clean
            // replay's note is `passive`.
            .filter(|wake| *wake == "wake")
            .count(),
        1,
        "only the recorded conflict woke the owner"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_base_move_after_the_carry_plan_is_reproven_and_replanned_onto_the_exact_new_base() {
    let h = harness_or_skip!("explicit_second_move");
    let (d1, _) = resolved_conflict(&h).await;
    let (jj, ws, store) = (h.jj.clone(), h.main_ws.clone(), h.store.clone());
    let moved = Arc::new(Mutex::new(None));
    let record = moved.clone();
    let _hook = set_hook(&h, move |attempt| {
        if attempt == 1 {
            let _ = crate::jj::update_stale(&jj, &ws);
            std::fs::write(ws.join("later.txt"), "later\n").unwrap();
            crate::jj::seal(&jj, &ws, "D2 lands elsewhere", None).unwrap();
            let tip = crate::jj::bookmark_commit(&jj, &store, MAIN_LINE).unwrap();
            jj.run(
                &store,
                &[
                    "bookmark",
                    "set",
                    "main",
                    "-r",
                    &tip,
                    "--ignore-working-copy",
                ],
                "race main",
            )
            .unwrap();
            *record.lock().unwrap() = Some(tip);
        }
    });

    let answer = request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, true, None)
        .await
        .unwrap();
    let d2 = moved.lock().unwrap().clone().expect("the hook moved main");
    assert!(
        answer.contains(&format!("from `{d1}` to `{d2}`")) && answer.contains("re-planned"),
        "the race is named, not read as a failed resolution: {answer}"
    );
    assert!(answer.contains("Replayed"), "{answer}");
    let tip = h.tip(BRANCH);
    assert!(h.is_ancestor(&d2, &tip), "landed on the exact newer base");
    assert_eq!(show(&h, &tip, "shared.txt"), "original\nA\nX");
    assert_eq!(show(&h, &tip, "later.txt"), "later");
    assert!(load_active_session(&h.db, BRANCH).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_base_that_outruns_the_resolution_rerecords_the_session_and_installs_nothing() {
    let h = harness_or_skip!("explicit_outrun");
    let (d1, resolved) = resolved_conflict(&h).await;
    // D2 appends at the very spot the owner resolved against D1.
    let d2 = h.advance_main("shared.txt", "original\nA\nB\n");

    let answer = request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, true, None)
        .await
        .unwrap();
    assert!(answer.contains("still conflicts"), "{answer}");
    assert!(
        answer.contains("was not carried onto the new base") && answer.contains("shared.txt"),
        "names the file the carry refused: {answer}"
    );
    assert_eq!(h.tip(BRANCH), resolved, "nothing installed");
    assert!(!h.is_ancestor(&d2, &resolved));
    let session = load_active_session(&h.db, BRANCH)
        .await
        .unwrap()
        .expect("a fresh session against D2");
    assert_eq!(session.theirs.as_deref(), Some(d2.as_str()));
    assert_eq!(
        session
            .resolution_basis
            .get("shared.txt")
            .map(|side| side.theirs.clone()),
        Some(d1),
        "it still knows the tip was resolved against D1"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn an_automatic_cooldown_never_holds_an_explicit_replay_and_a_starved_step_records_none() {
    let h = harness_or_skip!("explicit_cooldown");
    let own = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    let base = h.advance_main("incoming.txt", "theirs\n");
    crate::orchestrator::base_advance::record_reconcile_timeout(
        &h.db,
        "proj-1",
        &h.store,
        BRANCH,
        &own,
        &base,
        "reconcile_timeout: an automatic attempt timed out",
    )
    .await
    .unwrap();
    let quarantine = || async {
        crate::orchestrator::base_advance::load_reconcile_quarantine(
            &h.db, "proj-1", &h.store, BRANCH,
        )
        .await
        .unwrap()
        .map(|q| (q.fingerprint, q.strike_count, q.updated_at))
    };
    let before = quarantine().await.expect("the automatic cooldown holds");
    assert!(
        matches!(
            h.prepare(PreparationPurpose::Check).await.unwrap_err(),
            PreparationRefusal::Cooldown { .. }
        ),
        "automatic demand respects the cooldown"
    );

    // A step the explicit budget cannot cover is not started (the first is
    // base resolution, which already draws on the deadline), and records
    // nothing that would hold the next request.
    let answer = replay(
        &h.orch,
        &h.db,
        &ExplicitReplay {
            job_id: JOB,
            branch: BRANCH,
            expected_fingerprint: None,
            take_committed_tip: false,
            drop_incoming_reason: None,
            budget: Duration::from_millis(200),
            step_minimum: Duration::from_secs(1),
            recovered: None,
        },
    )
    .await
    .unwrap_err();
    assert!(
        answer.contains("stopped before the base resolution")
            && answer.contains("0.2s explicit budget")
            && answer.contains("not started")
            && answer.contains("No automatic cooldown was recorded"),
        "{answer}"
    );
    assert_eq!(h.tip(BRANCH), own);
    assert_eq!(
        quarantine().await,
        Some(before),
        "no strike, no new cooldown"
    );

    // The owner's request is served at once, despite the automatic cooldown.
    let answer = request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, false, None)
        .await
        .unwrap();
    assert!(answer.starts_with("Replayed"), "{answer}");
    assert!(h.is_ancestor(&base, &h.tip(BRANCH)));
    assert_eq!(
        quarantine().await,
        None,
        "the install releases the cooldown"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_held_branch_refuses_the_explicit_replay_before_accepting_anything() {
    let h = harness_or_skip!("explicit_held");
    let own = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    h.advance_main("incoming.txt", "theirs\n");
    for sql in [
        format!("INSERT INTO sessions (id,job_id,status,backend_id,created_at,updated_at) VALUES ('session-held','{JOB}','open','backend',1,1)"),
        "UPDATE runs SET session_id='session-held' WHERE id='run-prep'".to_string(),
        format!("INSERT INTO turns (id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES ('turn-held','session-held','run-prep','{JOB}',1,'yielded','initial',1,1)"),
        format!("INSERT INTO agent_waits (id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,state,created_at) VALUES ('wait-held','{JOB}','run-prep','session-held','turn-held','tool-held','{{\"kind\":\"run_batch\",\"request_id\":\"request-held\",\"commits\":true,\"label\":\"suite\"}}','pending',1)"),
    ] {
        h.db.execute(&sql, ()).await.unwrap();
    }
    assert!(
        crate::orchestrator::base_advance::inflight_run_batch_on_branch(&h.db, JOB, BRANCH)
            .await
            .unwrap()
            .is_some(),
        "the fixture's batch holds the branch"
    );

    let refusal = request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, false, None)
        .await
        .unwrap_err();
    assert!(
        refusal.contains("request-held") && refusal.contains("no replay was accepted"),
        "{refusal}"
    );
    assert_eq!(h.tip(BRANCH), own, "nothing rewritten under the batch");
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_branch_already_on_its_base_answers_and_closes_the_absorbed_session() {
    let h = harness_or_skip!("explicit_absorbed");
    let refusal = request_branch_replay(&h.orch, &h.db, JOB, BRANCH, Some("fp"), false, None)
        .await
        .unwrap_err();
    assert!(refusal.contains("no open rebase session"), "{refusal}");

    let own = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    let answer = request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, false, None)
        .await
        .unwrap();
    assert!(answer.contains("already carries `main`"), "{answer}");
    assert_eq!(h.tip(BRANCH), own, "nothing to replay moves nothing");
    assert!(h.messages().await.is_empty(), "and notifies no one");
}

fn short(budget: Duration) -> ExplicitReplay<'static> {
    ExplicitReplay {
        job_id: JOB,
        branch: BRANCH,
        expected_fingerprint: None,
        take_committed_tip: false,
        drop_incoming_reason: None,
        budget,
        step_minimum: Duration::ZERO,
        recovered: None,
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_held_store_guard_is_waited_on_only_within_the_explicit_deadline() {
    let h = harness_or_skip!("explicit_guard_budget");
    let own = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    h.advance_main("incoming.txt", "theirs\n");
    // The lock refuses re-entry from the task holding it, so another task
    // holds it, as a real competing writer would.
    let (held_tx, held_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let (orch, store) = (h.orch.clone(), h.store.clone());
    let holder = tokio::spawn(async move {
        let _guard = orch
            .acquire_jj_store_lock(
                &store,
                crate::orchestrator::StoreLockClass::Foreground,
                "test holder",
            )
            .await;
        held_tx.send(()).unwrap();
        let _ = release_rx.await;
    });
    held_rx.await.unwrap();
    let started = Instant::now();
    let refusal = replay(&h.orch, &h.db, &short(Duration::from_secs(2)))
        .await
        .unwrap_err();
    let waited = started.elapsed();
    release_tx.send(()).unwrap();
    holder.await.unwrap();
    assert!(
        waited < Duration::from_secs(10),
        "the 600s store-lock default never applies: {waited:?}"
    );
    assert!(
        refusal.contains("no replay was accepted") && refusal.contains("No automatic cooldown"),
        "{refusal}"
    );
    assert_eq!(h.tip(BRANCH), own);
    assert!(
        crate::orchestrator::base_advance::load_reconcile_quarantine(
            &h.db, "proj-1", &h.store, BRANCH
        )
        .await
        .unwrap()
        .is_none(),
        "a busy explicit replay records no cooldown"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_held_branch_reservation_is_waited_on_only_within_the_explicit_deadline() {
    let h = harness_or_skip!("explicit_reservation_budget");
    let own = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    h.advance_main("incoming.txt", "theirs\n");
    let reservation = h
        .orch
        .reserve_branches(&h.store, &[BRANCH], "test landing", Duration::ZERO)
        .await
        .unwrap();
    let started = Instant::now();
    let refusal = replay(&h.orch, &h.db, &short(Duration::from_secs(2)))
        .await
        .unwrap_err();
    let waited = started.elapsed();
    drop(reservation);
    assert!(
        waited < Duration::from_secs(10),
        "the reservation wait is clamped to the deadline: {waited:?}"
    );
    assert!(
        refusal.contains("stayed reserved") && refusal.contains("no replay was accepted"),
        "{refusal}"
    );
    assert_eq!(h.tip(BRANCH), own);

    // Released, the same request lands at once.
    let answer = request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, false, None)
        .await
        .unwrap();
    assert!(answer.starts_with("Replayed"), "{answer}");
}

/// The frozen result of the newest request recorded for `bookmark`.
async fn served_result(h: &Harness, bookmark: &str) -> Option<String> {
    h.db.query_text(
        "SELECT replay_result_json FROM jj_reconcile_items
          WHERE bookmark = ?1 AND replay_request_id IS NOT NULL
          ORDER BY replay_requested_at DESC, rowid DESC LIMIT 1",
        (bookmark.to_string(),),
    )
    .await
    .unwrap()
}

async fn unserved(h: &Harness) -> Vec<String> {
    conflict_session::list_pending_replay_requests(&h.db)
        .await
        .unwrap()
        .into_iter()
        .map(|listed| listed.bookmark)
        .collect()
}

async fn hold_branch(h: &Harness) {
    for sql in [
        format!("INSERT INTO sessions (id,job_id,status,backend_id,created_at,updated_at) VALUES ('session-held','{JOB}','open','backend',1,1)"),
        "UPDATE runs SET session_id='session-held' WHERE id='run-prep'".to_string(),
        format!("INSERT INTO turns (id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES ('turn-held','session-held','run-prep','{JOB}',1,'yielded','initial',1,1)"),
        format!("INSERT INTO agent_waits (id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,state,created_at) VALUES ('wait-held','{JOB}','run-prep','session-held','turn-held','tool-held','{{\"kind\":\"run_batch\",\"request_id\":\"request-held\",\"commits\":true,\"label\":\"suite\"}}','pending',1)"),
    ] {
        h.db.execute(&sql, ()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn an_accepted_request_is_recorded_once_and_served_with_its_landed_verdict() {
    let h = harness_or_skip!("explicit_marker_served");
    h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    let base = h.advance_main("incoming.txt", "theirs\n");

    let answer = request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, false, None)
        .await
        .unwrap();
    assert!(answer.starts_with("Replayed"), "{answer}");
    assert!(h.is_ancestor(&base, &h.tip(BRANCH)));
    assert!(unserved(&h).await.is_empty(), "the verdict consumed it");
    let result = served_result(&h, BRANCH).await.expect("a frozen verdict");
    assert!(
        result.contains("rebased_clean") && result.contains(&base),
        "served on the coordinate actually judged: {result}"
    );
    // The marker never made the preparation refuse its own coordinate.
    assert!(
        !answer.contains("background reconcile still owns"),
        "{answer}"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_request_held_by_a_run_batch_stays_unserved_until_the_batch_releases_the_branch() {
    let h = harness_or_skip!("explicit_marker_held");
    let own = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    let base = h.advance_main("incoming.txt", "theirs\n");
    hold_branch(&h).await;

    let refusal = request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, false, None)
        .await
        .unwrap_err();
    assert!(
        refusal.contains("request-held") && refusal.contains("Your request is recorded"),
        "{refusal}"
    );
    assert_eq!(h.tip(BRANCH), own);
    assert_eq!(
        unserved(&h).await,
        vec![BRANCH.to_string()],
        "visible, not lost"
    );

    // A release that does not end the hold serves nothing.
    serve_pending_replay_requests(&h.orch, Some((&h.store, BRANCH))).await;
    assert_eq!(h.tip(BRANCH), own);
    assert_eq!(unserved(&h).await, vec![BRANCH.to_string()]);

    h.db.execute(
        "UPDATE agent_waits SET state = 'resolved' WHERE id = 'wait-held'",
        (),
    )
    .await
    .unwrap();
    serve_pending_replay_requests(&h.orch, Some((&h.store, BRANCH))).await;
    assert!(h.is_ancestor(&base, &h.tip(BRANCH)), "served once released");
    assert!(unserved(&h).await.is_empty());
    assert!(served_result(&h, BRANCH)
        .await
        .unwrap()
        .contains("rebased_clean"));
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn recovery_serves_the_exact_branch_and_never_replays_a_moved_or_malformed_request() {
    let h = harness_or_skip!("explicit_marker_recovery");
    let own = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    let sibling_ws = h.workspaces.path().join("dormant");
    crate::jj::add_workspace(&h.jj, &h.store, &sibling_ws, SIBLING, "main", None).unwrap();
    h.db.execute_script(format!(
        "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
           VALUES ('job-dormant', 'exec-1', 'dormant', 'issue-1', 'proj-1', 'running', '{SIBLING}', NULL, 2, 2);"
    ))
    .await
    .unwrap();
    // The sibling's owner is live: it has a run a note can reach.
    h.db.execute_script(
        "INSERT INTO runs (id, issue_id, project_id, job_id, status, created_at, updated_at)
           VALUES ('run-dormant', 'issue-1', 'proj-1', 'job-dormant', 'live', 2, 2);",
    )
    .await
    .unwrap();
    let accepted_at = h.commit(&sibling_ws, SIBLING, "dormant.txt", "one\n");
    let base = h.advance_main("incoming.txt", "theirs\n");

    // Requests a previous process accepted and never served: one at the
    // branch's tip under an intent the queue retirement superseded, one whose
    // branch has moved on since, and one with an unreadable resolution.
    let queue = |branch: &'static str, tip: String| {
        let (db, store, base) = (h.db.clone(), h.store.clone(), base.clone());
        async move {
            conflict_session::queue_replay_request(
                &db,
                "proj-1",
                &store,
                "main",
                &base,
                branch,
                Some(&tip),
                false,
                None,
            )
            .await
            .unwrap()
        }
    };
    let ours = queue(BRANCH, own.clone()).await;
    h.db.execute(
        "UPDATE jj_reconcile_intents SET status = 'superseded' WHERE id = ?1",
        (ours.intent_id.clone(),),
    )
    .await
    .unwrap();
    queue(SIBLING, accepted_at.clone()).await;
    let moved = h.commit(&sibling_ws, SIBLING, "dormant.txt", "two\n");
    let malformed = queue(
        "agent/malformed",
        "0000000000000000000000000000000000000000".into(),
    )
    .await;
    h.db.execute(
        "UPDATE jj_reconcile_items SET replay_resolution = 'whatever'
          WHERE replay_request_id = ?1",
        (malformed.id.clone(),),
    )
    .await
    .unwrap();

    serve_pending_replay_requests(&h.orch, None).await;

    assert!(
        h.is_ancestor(&base, &h.tip(BRANCH)),
        "the accepted request landed"
    );
    assert!(served_result(&h, BRANCH)
        .await
        .unwrap()
        .contains("rebased_clean"));
    assert_eq!(h.tip(SIBLING), moved, "a moved branch is never replayed");
    assert!(!h.is_ancestor(&base, &moved));
    let superseded = served_result(&h, SIBLING).await.unwrap();
    assert!(
        superseded.contains("superseded")
            && superseded.contains(&accepted_at)
            && superseded.contains(&moved),
        "names the tip it was accepted at and where the branch is: {superseded}"
    );
    // The moved branch's owner learns why, once, without being woken.
    let notes = h.messages().await;
    assert_eq!(
        notes
            .iter()
            .filter(|(content, _)| content.contains("could not be served"))
            .count(),
        1,
        "{notes:?}"
    );
    serve_pending_replay_requests(&h.orch, None).await;
    assert_eq!(
        h.messages()
            .await
            .iter()
            .filter(|(content, _)| content.contains("could not be served"))
            .count(),
        1,
        "keyed by the request: delivered once"
    );
    assert!(
        !h.wakes()
            .await
            .iter()
            .any(|wake| wake == "wake" || wake == "interrupt"),
        "passive only"
    );
    let refused = served_result(&h, "agent/malformed").await.unwrap();
    assert!(
        refused.contains("malformed") && refused.contains("unknown resolution"),
        "never read as plain: {refused}"
    );
    assert!(unserved(&h).await.is_empty());
}

async fn request_rows(h: &Harness) -> Vec<(Option<i64>, Option<String>)> {
    h.db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT replay_served_at, replay_result_json FROM jj_reconcile_items
                      WHERE bookmark = ?1 AND replay_request_id IS NOT NULL
                      ORDER BY replay_requested_at, rowid",
                    (BRANCH,),
                )
                .await?;
            let mut all = Vec::new();
            while let Some(row) = rows.next().await? {
                all.push((row.opt_i64(0)?, row.opt_text(1)?));
            }
            Ok::<_, crate::storage::DbError>(all)
        })
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_newer_request_supersedes_an_older_unserved_one_and_alone_is_served() {
    let h = harness_or_skip!("explicit_marker_supersession");
    h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    h.advance_main("incoming.txt", "first\n");
    hold_branch(&h).await;
    request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, false, None)
        .await
        .unwrap_err();
    // The base moves, so the second request is recorded at a new coordinate.
    let second_base = h.advance_main("later.txt", "second\n");
    request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, false, None)
        .await
        .unwrap_err();

    let rows = request_rows(&h).await;
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert!(
        rows[0].0.is_some() && rows[0].1.is_none(),
        "the older request was superseded, served without a verdict: {rows:?}"
    );
    assert!(
        rows[1].0.is_none(),
        "the newest is the only authority: {rows:?}"
    );
    assert_eq!(unserved(&h).await, vec![BRANCH.to_string()]);

    h.db.execute(
        "UPDATE agent_waits SET state = 'resolved' WHERE id = 'wait-held'",
        (),
    )
    .await
    .unwrap();
    serve_pending_replay_requests(&h.orch, Some((&h.store, BRANCH))).await;
    assert!(h.is_ancestor(&second_base, &h.tip(BRANCH)));
    let rows = request_rows(&h).await;
    assert!(
        rows[0].1.is_none(),
        "the superseded request gets no verdict"
    );
    assert!(rows[1]
        .1
        .as_deref()
        .is_some_and(|result| result.contains("rebased_clean")));
    assert!(unserved(&h).await.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn the_hold_released_seam_serves_only_once_nothing_holds_the_branch() {
    let h = harness_or_skip!("explicit_marker_hook");
    h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    let base = h.advance_main("incoming.txt", "theirs\n");
    hold_branch(&h).await;
    request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, false, None)
        .await
        .unwrap_err();

    // Called while the batch still holds the branch: it serves nothing.
    on_branch_hold_released(&h.orch, JOB);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(unserved(&h).await, vec![BRANCH.to_string()]);

    // A Stop cancels the run's waits and calls the by-run seam, which returns
    // at once and serves the job's exact branch on a spawned task.
    h.db.execute(
        "UPDATE agent_waits SET state = 'cancelled' WHERE run_id = 'run-prep'",
        (),
    )
    .await
    .unwrap();
    on_branch_hold_released_for_run(&h.orch, "run-prep");
    let started = Instant::now();
    while !unserved(&h).await.is_empty() {
        assert!(
            started.elapsed() < Duration::from_secs(120),
            "the spawned service never served the request"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(h.is_ancestor(&base, &h.tip(BRANCH)));
    assert!(served_result(&h, BRANCH)
        .await
        .unwrap()
        .contains("rebased_clean"));
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_stood_down_batch_serves_replay_while_its_wait_for_sibling_stays_parked() {
    use crate::mcp::handlers::durable_suspend::{resolve, Condition, Record};

    let h = harness_or_skip!("explicit_marker_stood_down");
    h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    let base = h.advance_main("incoming.txt", "theirs\n");
    hold_branch(&h).await;
    h.db.execute(
        "UPDATE jobs SET current_session_id='session-held', current_turn_id='turn-held' WHERE id=?1",
        (JOB,),
    )
    .await
    .unwrap();
    let sibling = serde_json::to_string(&Condition::Duration).unwrap();
    h.db.execute(
        "INSERT INTO agent_waits (id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,state,created_at)
         VALUES ('wait-sibling',?1,'run-prep','session-held','turn-held','tool-sibling',?2,'pending',1)",
        (JOB, sibling),
    )
    .await
    .unwrap();
    let held =
        h.db.query_text(
            "SELECT condition_json FROM agent_waits WHERE id='wait-held'",
            (),
        )
        .await
        .unwrap()
        .unwrap();
    let record = Record {
        id: "wait-held".into(),
        job_id: JOB.into(),
        run_id: "run-prep".into(),
        session_id: "session-held".into(),
        turn_id: "turn-held".into(),
        tool_use_id: "tool-held".into(),
        condition: serde_json::from_str(&held).unwrap(),
        deadline: None,
        created: 1,
    };
    request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, false, None)
        .await
        .unwrap_err();
    assert_eq!(unserved(&h).await, vec![BRANCH.to_string()]);

    // Only the batch resolves. Its sibling cannot drive a later release hook.
    resolve(&h.orch, &h.db, &record, "batch finished", false)
        .await
        .unwrap();
    assert_eq!(
        h.db.query_text("SELECT state FROM agent_waits WHERE id='wait-held'", ())
            .await
            .unwrap()
            .as_deref(),
        Some("resolved"),
    );
    let started = Instant::now();
    while !unserved(&h).await.is_empty() {
        assert!(
            started.elapsed() < Duration::from_secs(120),
            "the stood-down resolver never served the deferred replay"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        h.db.query_text("SELECT state FROM agent_waits WHERE id='wait-sibling'", ())
            .await
            .unwrap()
            .as_deref(),
        Some("pending"),
        "replay does not wait for the waitFor sibling",
    );
    assert_eq!(
        h.db.query_one(
            "SELECT successor_turn_id FROM agent_waits WHERE id='wait-held'",
            (),
            |row| row.opt_text(0),
        )
        .await
        .unwrap(),
        None,
        "the batch stood down rather than resuming the turn",
    );
    assert!(h.is_ancestor(&base, &h.tip(BRANCH)));
    assert!(served_result(&h, BRANCH)
        .await
        .unwrap()
        .contains("rebased_clean"));
}

/// An MCP item that asks for input takes its batch's call over, turning the
/// batch's wait row into a continuation in place. That row no longer holds the
/// branch and will never resolve as a batch, so the takeover is the exit that
/// must serve the replay deferred behind it. A row that never held the branch
/// (a host batch) releases nothing while another batch still holds it.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_takeover_of_a_branch_holding_batch_serves_its_deferred_replay() {
    use crate::mcp::handlers::durable_suspend::{take_over_run_batch, Condition, Record};

    let h = harness_or_skip!("explicit_marker_takeover");
    h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    let base = h.advance_main("incoming.txt", "theirs\n");
    hold_branch(&h).await;
    let host = Condition::RunBatch {
        request_id: "request-host".into(),
        commits: false,
        label: None,
        host: true,
        identity: None,
        verdict_only: false,
        settlement: None,
    };
    assert!(!host.holds_branch());
    h.db.execute(
        "INSERT INTO agent_waits (id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,kind,state,created_at)
         VALUES ('wait-host',?1,'run-prep','session-held','turn-held','tool-host',?2,'run_batch','pending',1)",
        (JOB, serde_json::to_string(&host).unwrap()),
    )
    .await
    .unwrap();
    // A takeover converts only a row its writer tagged as a batch's wait.
    h.db.execute(
        "UPDATE agent_waits SET kind='run_batch' WHERE id='wait-held'",
        (),
    )
    .await
    .unwrap();
    let held: Condition = serde_json::from_str(
        &h.db
            .query_text(
                "SELECT condition_json FROM agent_waits WHERE id='wait-held'",
                (),
            )
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert!(held.holds_branch());
    let record = |id: &str, tool_use_id: &str, condition: Condition| Record {
        id: id.into(),
        job_id: JOB.into(),
        run_id: "run-prep".into(),
        session_id: "session-held".into(),
        turn_id: "turn-held".into(),
        tool_use_id: tool_use_id.into(),
        condition,
        deadline: None,
        created: 1,
    };
    let continuation = || Condition::McpContinuation {
        state: serde_json::from_value(serde_json::json!({
            "server": "fake",
            "session_key": JOB,
            "config": {},
            "tool": "durable",
        }))
        .unwrap(),
    };
    request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, false, None)
        .await
        .unwrap_err();
    assert_eq!(unserved(&h).await, vec![BRANCH.to_string()]);

    take_over_run_batch(
        &h.orch,
        &h.db,
        &record("wait-host", "tool-host", host),
        continuation(),
        None,
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        unserved(&h).await,
        vec![BRANCH.to_string()],
        "the batch that holds the branch still holds it"
    );

    take_over_run_batch(
        &h.orch,
        &h.db,
        &record("wait-held", "tool-held", held),
        continuation(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        h.db.query_text("SELECT kind FROM agent_waits WHERE id='wait-held'", ())
            .await
            .unwrap()
            .as_deref(),
        Some("mcp_continuation"),
        "the batch row became the continuation rather than resolving",
    );
    let started = Instant::now();
    while !unserved(&h).await.is_empty() {
        assert!(
            started.elapsed() < Duration::from_secs(120),
            "the takeover never served the deferred replay"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(h.is_ancestor(&base, &h.tip(BRANCH)));
    assert!(served_result(&h, BRANCH)
        .await
        .unwrap()
        .contains("rebased_clean"));
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn an_expected_head_refuses_a_capture_at_any_other_head_under_the_guard() {
    let h = harness_or_skip!("explicit_expected_head");
    let accepted = h.commit(&h.branch_ws, BRANCH, "own.txt", "one\n");
    h.advance_main("incoming.txt", "theirs\n");
    let moved = h.commit(&h.branch_ws, BRANCH, "own.txt", "two\n");
    let mut request = PreparationRequest::new(
        "proj-1",
        BRANCH,
        Some(JOB.to_string()),
        PreparationPurpose::OwnerCommit,
    );
    request.explicit_replay = true;
    request.expected_head = Some(accepted.clone());
    let refusal = prepare_branch(&h.orch, request).await.unwrap_err();
    assert!(
        matches!(&refusal, PreparationRefusal::Moved { what } if what.contains(&accepted) && what.contains(&moved)),
        "{refusal}"
    );
    assert_eq!(h.tip(BRANCH), moved, "the newer commit is never replayed");
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn concurrent_servers_attempt_one_request_once() {
    let h = harness_or_skip!("explicit_marker_single_flight");
    let own = h.commit(&h.branch_ws, BRANCH, "shared.txt", "mine\n");
    let base = h.advance_main("shared.txt", "theirs\n");
    conflict_session::queue_replay_request(
        &h.db,
        "proj-1",
        &h.store,
        "main",
        &base,
        BRANCH,
        Some(&own),
        false,
        None,
    )
    .await
    .unwrap();

    tokio::join!(
        serve_pending_replay_requests(&h.orch, None),
        serve_pending_replay_requests(&h.orch, Some((&h.store, BRANCH))),
    );

    // The conflict is recorded by exactly one attempt.
    let attempts =
        h.db.query_text(
            "SELECT CAST(MAX(attempt_count) AS TEXT) FROM jj_reconcile_items
              WHERE bookmark = ?1 AND outcome_kind = 'conflicted'",
            (BRANCH,),
        )
        .await
        .unwrap();
    assert_eq!(attempts.as_deref(), Some("1"), "one attempt, not two");
    assert_eq!(
        h.wakes()
            .await
            .iter()
            .filter(|wake| *wake == "wake")
            .count(),
        1
    );
    assert!(unserved(&h).await.is_empty(), "the conflict is its verdict");
    assert_eq!(h.tip(BRANCH), own);
}

/// A duplicate claim is refused at once, never touches the held one, and the
/// holder's release admits the next. Bounded on its own thread: the defect
/// this pins was a self-deadlock.
#[test]
fn a_held_serving_claim_refuses_a_second_without_releasing_the_first() {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let first = ServingClaim::take("/claim-store", "agent/claim").unwrap();
        let refused = ServingClaim::take("/claim-store", "agent/claim").is_none();
        let kept = ServingClaim::take("/claim-store", "agent/claim").is_none();
        drop(first);
        let next = ServingClaim::take("/claim-store", "agent/claim").is_some();
        tx.send((refused, kept, next)).unwrap();
    });
    let (refused, kept, next) = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("claiming never deadlocks");
    assert!(refused, "a duplicate claim is refused");
    assert!(kept, "and refusing it does not release the held claim");
    assert!(next, "the holder's release admits the next server");
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn recovery_serves_a_team_replica_request_end_to_end_in_its_own_database() {
    let h = harness_or_skip!("explicit_marker_team");
    const PROJECT: &str = "team1~00000000-0000-4000-8000-0000000000a1";
    const ISSUE: &str = "team1~00000000-0000-4000-8000-0000000000a2";
    const EXECUTION: &str = "team1~00000000-0000-4000-8000-0000000000a3";
    const TEAM_JOB: &str = "team1~00000000-0000-4000-8000-0000000000a4";
    const TEAM_RUN: &str = "team1~00000000-0000-4000-8000-0000000000a5";
    const TEAM_BRANCH: &str = "agent/team-replay";
    let team = Arc::new(crate::storage::migrated_test_db("explicit-marker-team.db").await);
    team.execute_script(format!(
        "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
           VALUES ('{PROJECT}', 'default', 'Team', 'team', '/another-machine/team', 'main', 1, 1);
         INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
           VALUES ('{ISSUE}', '{PROJECT}', 1, 'Issue', 'active', 1, 1);
         INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
           VALUES ('{EXECUTION}', 'recipe-default', '{ISSUE}', '{PROJECT}', 'running', 1, 1);
         INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
           VALUES ('{TEAM_JOB}', '{EXECUTION}', 'node', '{ISSUE}', '{PROJECT}', 'running', '{TEAM_BRANCH}', NULL, 1, 1);
         INSERT INTO runs (id, issue_id, project_id, job_id, status, created_at, updated_at)
           VALUES ('{TEAM_RUN}', '{ISSUE}', '{PROJECT}', '{TEAM_JOB}', 'live', 1, 1);"
    ))
    .await
    .unwrap();
    h.orch
        .db
        .insert_team_db_for_test("team1", team.clone())
        .await;
    // A clone path updates an existing private route, as it does after a team
    // project is discovered. The synced path must never select this store.
    h.db.execute_script(
        "INSERT INTO teams (id, name, sync_url, replica_path, created_at)
           VALUES ('team1', 'Team', 'http://sync', '/tmp/team.db', 1);
         INSERT INTO project_routes (project_key, team_id, created_at)
           VALUES ('team', 'team1', 1);",
    )
    .await
    .unwrap();
    crate::projects::crud::set_local_repo_path(&h.orch.db.local, "team", &h.repo)
        .await
        .unwrap();
    let (local_repo, key) =
        crate::projects::crud::resolve_local_repo_path_and_key(&h.orch.db, PROJECT)
            .await
            .unwrap();
    assert_eq!(key, "team");
    assert_eq!(local_repo.as_deref(), h.repo.to_str());

    let team_ws = h.workspaces.path().join("team");
    crate::jj::add_workspace(&h.jj, &h.store, &team_ws, TEAM_BRANCH, "main", None).unwrap();
    let accepted_at = h.commit(&team_ws, TEAM_BRANCH, "team.txt", "team\n");
    let base = h.advance_main("incoming.txt", "theirs\n");
    let request = conflict_session::queue_replay_request(
        &team,
        PROJECT,
        &h.store,
        "main",
        &base,
        TEAM_BRANCH,
        Some(&accepted_at),
        false,
        None,
    )
    .await
    .unwrap();
    assert!(
        unserved(&h).await.is_empty(),
        "the local database holds nothing"
    );

    serve_pending_replay_requests(&h.orch, None).await;

    assert!(
        h.is_ancestor(&base, &h.tip(TEAM_BRANCH)),
        "the replica's request was replayed through the replica"
    );
    let conflict_session::ReplayRequestState::Served { pass, destination } =
        conflict_session::replay_request_state(&team, &request)
            .await
            .unwrap()
    else {
        panic!("the replica's request is served in the replica");
    };
    assert_eq!(pass.outcome_kind.as_deref(), Some("rebased_clean"));
    assert_eq!(destination, base);
    assert!(conflict_session::list_pending_replay_requests(&team)
        .await
        .unwrap()
        .is_empty());
    let local_items =
        h.db.query_text(
            "SELECT CAST(COUNT(*) AS TEXT) FROM jj_reconcile_items WHERE bookmark = ?1",
            (TEAM_BRANCH,),
        )
        .await
        .unwrap();
    assert_eq!(
        local_items.as_deref(),
        Some("0"),
        "nothing about the replica's branch lands in the private database"
    );
}

/// The real `cairn:~/rebase` page of the harness job, addressed by its node.
async fn rebase_page(h: &Harness) -> String {
    h.db.execute(
        "UPDATE jobs SET uri_segment = 'builder' WHERE id = ?1",
        (JOB,),
    )
    .await
    .unwrap();
    crate::resources::read_node_rebase(&h.orch, "proj", 1, 1, "builder", &[]).await
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn the_rebase_page_shows_a_held_plain_request_that_has_no_session() {
    let h = harness_or_skip!("explicit_page_held");
    let own = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    let base = h.advance_main("incoming.txt", "theirs\n");
    hold_branch(&h).await;
    request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, false, None)
        .await
        .unwrap_err();
    assert!(load_active_session(&h.db, BRANCH).await.unwrap().is_none());

    let page = rebase_page(&h).await;
    assert!(
        page.contains("## Your replay request")
            && page.contains(&format!("accepted at `{own}` onto `main` at `{base}`"))
            && page.contains("**pending**")
            && page.contains("`request-held`")
            && page.contains("when that batch completes"),
        "{page}"
    );
    assert!(!page.contains("retries it automatically"), "{page}");
    assert!(!page.contains("durable reconcile worker"), "{page}");
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn the_rebase_page_keeps_a_closed_requests_frozen_reason_after_the_branch_and_base_move() {
    let h = harness_or_skip!("explicit_page_closed");
    let accepted = h.commit(&h.branch_ws, BRANCH, "own.txt", "one\n");
    let first_base = h.advance_main("incoming.txt", "first\n");
    conflict_session::queue_replay_request(
        &h.db,
        "proj-1",
        &h.store,
        "main",
        &first_base,
        BRANCH,
        Some(&accepted),
        false,
        None,
    )
    .await
    .unwrap();
    let moved = h.commit(&h.branch_ws, BRANCH, "own.txt", "two\n");
    serve_pending_replay_requests(&h.orch, None).await;
    h.advance_main("later.txt", "second\n");

    let page = rebase_page(&h).await;
    assert!(
        page.contains("## Your replay request")
            && page.contains(&format!(
                "accepted at `{accepted}` onto `main` at `{first_base}`"
            ))
            && page.contains("**closed**")
            && page.contains(&format!("moved from {accepted} to {moved}"))
            && page.contains("Request it again"),
        "the older base's frozen reason stays visible: {page}"
    );
    assert_eq!(h.tip(BRANCH), moved, "nothing was replayed");
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_recovered_request_refused_by_a_held_reservation_closes_once_with_one_passive_note() {
    let h = harness_or_skip!("explicit_marker_reserved");
    let own = h.commit(&h.branch_ws, BRANCH, "own.txt", "mine\n");
    let base = h.advance_main("incoming.txt", "theirs\n");
    conflict_session::queue_replay_request(
        &h.db,
        "proj-1",
        &h.store,
        "main",
        &base,
        BRANCH,
        Some(&own),
        false,
        None,
    )
    .await
    .unwrap();

    // Another task holds the branch's reservation, as a landing would.
    let (held_tx, held_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let (orch, store) = (h.orch.clone(), h.store.clone());
    let holder = tokio::spawn(async move {
        let _reservation = orch
            .reserve_branches(&store, &[BRANCH], "test landing", Duration::ZERO)
            .await
            .unwrap();
        held_tx.send(()).unwrap();
        let _ = release_rx.await;
    });
    held_rx.await.unwrap();

    let closing_note = |messages: Vec<(String, String)>| {
        messages
            .into_iter()
            .filter(|(content, _)| {
                content.contains("could not be served when the batch holding the branch ended")
                    && content.contains("Request it again from cairn:~/rebase")
            })
            .count()
    };
    let (served, closed, held) = serve_in(&h.orch, &h.db, None, Duration::from_secs(2)).await;
    assert_eq!((served, closed, held), (1, 0, 0), "attempted and settled");
    let result = served_result(&h, BRANCH)
        .await
        .expect("closed with its reason");
    assert!(
        result.contains("not_attempted") && result.contains("stayed reserved"),
        "{result}"
    );
    assert!(unserved(&h).await.is_empty(), "never left quietly unserved");
    assert_eq!(h.tip(BRANCH), own);
    assert_eq!(closing_note(h.messages().await), 1);

    // A second trigger finds nothing to serve and notes nothing again.
    serve_in(&h.orch, &h.db, None, Duration::from_secs(2)).await;
    assert_eq!(closing_note(h.messages().await), 1);
    assert!(
        !h.wakes()
            .await
            .iter()
            .any(|wake| wake == "wake" || wake == "interrupt"),
        "passive only"
    );
    release_tx.send(()).unwrap();
    holder.await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn merge_mode_explicit_branch_replay_merges_without_rewriting_prior_commits() {
    let h = harness_or_skip!("explicit_merge_mode");
    let first = h.commit(&h.branch_ws, BRANCH, "own.txt", "first\n");
    let second = h.commit(&h.branch_ws, BRANCH, "own.txt", "second\n");
    h.enable_merge_mode().await;
    let d1 = h.advance_main("incoming.txt", "first base\n");
    let prepared = h.prepare(PreparationPurpose::OwnerCommit).await.unwrap();
    h.assert_merge_advance(
        &second,
        &d1,
        &prepared.head_after,
        &[first.clone(), second.clone()],
    )
    .await;
    let d2 = h.advance_main("incoming.txt", "second base\n");

    let answer = request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, false, None)
        .await
        .unwrap();
    assert!(answer.contains("not a green build"), "{answer}");
    let installed = h.tip(BRANCH);
    h.assert_merge_advance(
        &prepared.head_after,
        &d2,
        &installed,
        &[first, second, prepared.head_after.clone()],
    )
    .await;
    assert_eq!(show(&h, &installed, "own.txt"), "second");
    assert_eq!(show(&h, &installed, "incoming.txt"), "second base");
    assert!(load_active_session(&h.db, BRANCH).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn merge_mode_explicit_resolution_carry_stays_anchored_to_the_session_destination() {
    let h = harness_or_skip!("explicit_merge_resolution_carry");
    h.enable_merge_mode().await;
    let (d1, resolved) = resolved_conflict(&h).await;
    let session = load_active_session(&h.db, BRANCH).await.unwrap().unwrap();
    assert_eq!(session.theirs.as_deref(), Some(d1.as_str()));
    let prior = git(&h.repo, &["rev-list", &resolved, "^main"])
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let d2 = h.advance_main("later.txt", "later\n");

    let answer = request_branch_replay(&h.orch, &h.db, JOB, BRANCH, None, true, None)
        .await
        .unwrap();
    let installed = h.tip(BRANCH);
    assert!(answer.contains("not a green build"), "{answer}");
    h.assert_merge_advance(&resolved, &d1, &installed, &prior)
        .await;
    assert!(
        !h.is_ancestor(&d2, &installed),
        "resolution merges its recorded destination, not live main"
    );
    assert_eq!(show(&h, &installed, "shared.txt"), "original\nA\nX");
    assert!(load_active_session(&h.db, BRANCH).await.unwrap().is_none());

    // Catch-up to the newer destination is a separate demand and merge, never
    // a retargeting of the owner's recorded resolution.
    let caught_up = h.prepare(PreparationPurpose::Check).await.unwrap();
    assert!(caught_up.advanced);
    let mut all_prior = prior;
    all_prior.push(installed.clone());
    h.assert_merge_advance(&installed, &d2, &caught_up.head_after, &all_prior)
        .await;
    assert_eq!(
        show(&h, &caught_up.head_after, "shared.txt"),
        "original\nA\nX"
    );
    assert_eq!(show(&h, &caught_up.head_after, "later.txt"), "later");
}
