use crate::common;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use cairn_core::internal::backends::replayed_anthropic_context_for_test;
use cairn_core::internal::db::DbState;
use cairn_core::internal::jj;
use cairn_core::internal::mcp::handlers::read::handle_read_file;
use cairn_core::internal::mcp::handlers::write::handle_write;
use cairn_core::internal::mcp::handlers::write::replay::{
    gate_resource_item, gate_settled_delivery, ItemPhase,
};
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::orchestrator::lifecycle;
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::services::{testing::TestServicesBuilder, RealGitClient};
use cairn_core::internal::storage::{LocalDb, RowExt, SearchIndex};
use cairn_core::memories::db as memory_db;
use cairn_core::models::{
    AgentSnapshot, ExecutionSnapshot, Fence, RecipeSnapshot, RecipeTrigger, TriggerContext,
    TriggerType,
};
use cairn_db::turso::params;
use serde_json::json;

// The change/write path seals and discards through the jj VCS seam, so a
// file-target test cwd must be a real `.jj` workspace over a shared store. These
// tests resolve `jj` and self-skip with a note when it is unavailable, mirroring
// `mcp::vcs`'s jj tests. Resource-only tests never touch the VCS and keep a
// plain cwd.

fn make_request(cwd: &str, payload: serde_json::Value) -> McpCallbackRequest {
    make_request_as_run(cwd, payload, None)
}

/// A delivery that died after sealing can leave its ledger row pending. A
/// redelivery takes ownership of that row and settles its already-sealed report
/// as the replay, without sealing the write a second time.
#[tokio::test]
async fn an_orphaned_ordinary_pending_report_settles_without_a_second_commit() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("ordinary-replay.txt", "tail\n");

    let payload = json!({
        "changes": [{
            "target": "file:ordinary-replay.txt",
            "mode": "patch",
            "payload": { "old_string": "tail", "new_string": "head\ntail" }
        }],
        "commit_msg": "land ordinary write"
    });
    let first = repo
        .change_report_as_tool_use("toolu_ordinary_crash", payload.clone())
        .await;
    assert_successful_change(&first, 1);
    let sealed_head = repo.branch_head();

    // Model the durable state left by a crash of the pending implementation.
    repo.orch
        .db
        .local
        .execute(
            "UPDATE write_replay_ledger
             SET finalized = 0, owner_token = 'crashed-owner', claimed_at = 0
             WHERE run_id = 'run-change' AND tool_use_id = 'toolu_ordinary_crash'",
            (),
        )
        .await
        .unwrap();

    let replay = repo
        .change_report_as_tool_use("toolu_ordinary_crash", payload)
        .await;
    assert_eq!(
        replay["commit"]["sha"], first["commit"]["sha"],
        "{replay:?}"
    );
    assert!(replay["replayed"].is_string(), "{replay:?}");
    assert_eq!(
        repo.branch_head(),
        sealed_head,
        "redelivery must not seal a second commit"
    );
    assert_eq!(repo.read("ordinary-replay.txt"), "head\ntail\n");

    let finalized = repo
        .orch
        .db
        .local
        .query_one(
            "SELECT finalized FROM write_replay_ledger
             WHERE run_id = 'run-change' AND tool_use_id = 'toolu_ordinary_crash'",
            (),
            |row| row.i64(0),
        )
        .await
        .unwrap();
    assert_eq!(finalized, 1, "redelivery must settle the orphaned row");
}

fn resumed_model_context(repo: &ChangeTestRepo) -> String {
    replayed_anthropic_context_for_test(
        &repo.orch,
        "session-change",
        "resumed-run",
        &repo.project_id,
        "CHG",
    )
    .expect("resumed provider context must reconstruct")
    .to_string()
}

async fn record_write_call(db: &LocalDb, tool_use_id: &str, payload: &serde_json::Value) {
    let data = json!({
        "eventType": "assistant",
        "sessionId": "session-change",
        "toolUses": [{"id": tool_use_id, "name": "mcp__cairn__write", "input": payload}],
        "isError": false
    })
    .to_string();
    db.execute(
        "INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data, created_at, turn_id)
         VALUES (?1, 'run-change', 'session-change', 1, 1, 'assistant', ?2, 1, 'turn-change')",
        params![format!("event-{tool_use_id}"), data],
    ).await.unwrap();
}

async fn stopped_result(db: &LocalDb, tool_use_id: &str) -> String {
    db.query_one(
        "SELECT data FROM events WHERE run_id = 'run-change' AND event_type = 'tool_result'
         AND json_extract(data, '$.toolUseId') = ?1",
        params![tool_use_id],
        |row| row.text(0),
    )
    .await
    .expect("stop must reconcile the settled delivery")
}

#[tokio::test]
async fn change_create_refuses_existing_tracked_file_without_mutation_or_commit() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("existing.txt", "original tracked bytes\n");
    repo.seal_base("track existing file");
    let head_before = repo.branch_head();

    let report = repo
        .change_report(json!({
            "changes": [{
                "target": "file:existing.txt",
                "mode": "create",
                "payload": { "content": "destructive overwrite\n" }
            }],
            "commit_msg": "must not commit"
        }))
        .await;

    assert_eq!(failure_count(&report), 1, "{report:?}");
    assert!(report["applied"].as_array().is_some_and(Vec::is_empty));
    assert!(report.get("commit").is_none(), "{report:?}");
    let error = report["failures"][0]["error"].as_str().unwrap();
    assert!(error.contains("target already exists"), "{error}");
    assert!(error.contains("mode=patch"), "{error}");
    assert!(error.contains("mode=replace"), "{error}");
    assert_eq!(repo.read("existing.txt"), "original tracked bytes\n");
    assert_eq!(repo.branch_head(), head_before);
}

#[tokio::test]
async fn change_create_refuses_existing_logical_target_atomically() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("logical.txt", "original logical bytes\n");
    let head_before = repo.branch_head();

    let report = repo
        .change_report(json!({
            "changes": [
                {
                    "target": "file:logical.txt",
                    "mode": "create",
                    "payload": { "content": "overwrite\n" }
                },
                {
                    "target": "file:must-not-exist.txt",
                    "mode": "create",
                    "payload": { "content": "sibling\n" }
                }
            ],
            "commit_msg": "must refuse whole file batch",
            "atomic": true
        }))
        .await;

    assert_eq!(failure_count(&report), 1, "{report:?}");
    assert!(report["applied"].as_array().is_some_and(Vec::is_empty));
    assert_eq!(repo.read("logical.txt"), "original logical bytes\n");
    assert!(!repo.logical_exists("must-not-exist.txt"));
    assert_eq!(repo.branch_head(), head_before);
}

#[tokio::test]
async fn change_create_adds_absent_target_and_replace_overwrites_deliberately() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };

    let created = repo
        .change_report(json!({
            "changes": [{
                "target": "file:intent.txt",
                "mode": "create",
                "payload": { "content": "created\n" }
            }],
            "commit_msg": "create absent target"
        }))
        .await;
    assert_successful_change(&created, 1);
    assert_eq!(created["applied"][0]["summary"], "+file:intent.txt");
    assert_eq!(repo.read("intent.txt"), "created\n");

    let replaced = repo
        .change_report(json!({
            "changes": [{
                "target": "file:intent.txt",
                "mode": "replace",
                "payload": { "content": "replaced\n" }
            }],
            "commit_msg": "replace existing target"
        }))
        .await;
    assert_successful_change(&replaced, 1);
    assert_eq!(replaced["applied"][0]["summary"], "~file:intent.txt");
    assert_eq!(repo.read("intent.txt"), "replaced\n");
}

fn make_request_as_run(
    cwd: &str,
    payload: serde_json::Value,
    run_id: Option<&str>,
) -> McpCallbackRequest {
    McpCallbackRequest {
        answer_bound_ms: None,
        thread_id: None,
        cwd: cwd.to_string(),
        run_id: run_id.map(str::to_string),
        tool: "write".to_string(),
        payload,
        tool_use_id: None,
        origin: None,
    }
}

#[tokio::test]
async fn logical_write_is_immediately_visible_to_ordinary_read_without_checkout_update() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("coherent.txt", "before\n");
    let report = repo
        .change_report(json!({
            "changes": [{
                "target": "file:coherent.txt",
                "mode": "patch",
                "payload": { "old_string": "before", "new_string": "after" }
            }],
            "commit_msg": "advance logical head"
        }))
        .await;
    assert_successful_change(&report, 1);

    assert_eq!(
        std::fs::read_to_string(repo.path("coherent.txt")).unwrap(),
        "before\n",
        "the retained checkout intentionally stays behind"
    );
    let read = handle_read_file(
        &repo.orch,
        &make_read_request(repo.cwd(), "file:coherent.txt"),
    )
    .await;
    assert!(read.contains("after"), "{read}");
    assert!(!read.contains("before"), "{read}");
}

/// A file write is prepared without the store lock and takes it only to seal
/// (cairn/5069). When the branch moves after the write has read it, the seal's
/// compare-and-set must send the write back to run under the lock: applying
/// the patch prepared against the old head would silently drop the line the
/// move added.
#[tokio::test]
async fn a_write_whose_branch_moves_before_its_seal_is_written_again_under_the_lock() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("notes.txt", "alpha\n");
    let orch = repo.orch.clone();
    let mut request = make_request(
        repo.cwd(),
        json!({
            "changes": [{
                "target": "file:notes.txt",
                "mode": "patch",
                "payload": { "old_string": "alpha", "new_string": "alpha-mine" }
            }],
            "commit_msg": "edit alpha"
        }),
    );
    request.run_id = Some("run-change".to_string());
    let id = "branch-moves-before-seal";
    record_write_call(&repo.orch.db.local, id, &request.payload).await;
    request.tool_use_id = Some(id.into());
    let (reached, resume) = cairn_core::internal::mcp::handlers::write::test_pause::before_seal(id);
    let write = tokio::spawn(async move { handle_write(&orch, &request).await });
    tokio::time::timeout(std::time::Duration::from_secs(60), reached.notified())
        .await
        .expect("the write has prepared its patch without retaining the store lock");

    let report = repo
        .change_report(json!({
            "changes": [{"target":"file:notes.txt", "mode":"patch",
                "payload":{"old_string":"alpha", "new_string":"alpha\nbeta"}}],
            "commit_msg":"concurrent owner publication"
        }))
        .await;
    assert_successful_change(&report, 1);
    resume.notify_one();

    let report = parse_report(&write.await.unwrap());
    assert_successful_change(&report, 1);
    assert_eq!(repo.read("notes.txt"), "alpha-mine\nbeta\n");
}

#[tokio::test]
async fn a_file_seal_checks_acceptance_debt_that_appeared_after_patch_interpretation() {
    for atomic in [false, true] {
        for status in ["unknown", "pending"] {
            let Some(repo) = ChangeTestRepo::try_new().await else {
                eprintln!("skipping: jj not resolvable");
                return;
            };
            repo.write("debt.txt", "before\n");
            let before = repo.branch_head();
            let store = jj::project_store_dir(repo.config.path(), repo.project.path());
            let store_key = std::fs::canonicalize(&store)
                .unwrap()
                .to_string_lossy()
                .into_owned();
            let env = jj::environment(&repo.jj_bin, repo.config.path());
            let base = jj::bookmark_commit(&env, &store, "main").unwrap();
            let id = format!("file-acceptance-debt-{atomic}-{status}");
            let mut request = make_request(
                repo.cwd(),
                json!({
                    "changes": [{"target":"file:debt.txt", "mode":"patch",
                        "payload":{"old_string":"before", "new_string":"after"}}],
                    "atomic": atomic,
                    "commit_msg": "publish only with settled acceptance"
                }),
            );
            request.run_id = Some("run-change".into());
            request.tool_use_id = Some(id.clone());
            record_write_call(&repo.orch.db.local, &id, &request.payload).await;
            let (reached, resume) =
                cairn_core::internal::mcp::handlers::write::test_pause::before_seal(&id);
            let orch = repo.orch.clone();
            let write = tokio::spawn(async move { handle_write(&orch, &request).await });
            tokio::time::timeout(std::time::Duration::from_secs(60), reached.notified())
                .await
                .expect("the patch has been interpreted without a writer guard");
            let guard = repo.orch.hold_jj_store_lock_for_test(&store).await;
            repo.orch
                .db
                .local
                .execute(
                    "INSERT INTO branch_advance_acceptances
                   (id, project_id, store_path, branch, head_before, head_after, base_branch,
                    base_commit, captured_operation, dropped_commits_json, inherited_drops_json,
                    status, created_at)
                 VALUES ('debt-at-seal', ?1, ?2, 'agent/CHG-1-builder-0', ?3, ?4,
                         'main', ?3, 'captured-op', '[]', '[]', ?5, 1)",
                    params![
                        repo.project_id.as_str(),
                        store_key.as_str(),
                        base.as_str(),
                        before.as_str(),
                        status
                    ],
                )
                .await
                .unwrap();
            drop(guard);
            resume.notify_one();
            let report = parse_report(&write.await.unwrap());

            if status == "pending" {
                assert_successful_change(&report, 1);
                assert_eq!(repo.read("debt.txt"), "after\n");
                let settled = repo
                    .orch
                    .db
                    .local
                    .query_one(
                        "SELECT status FROM branch_advance_acceptances WHERE id = 'debt-at-seal'",
                        (),
                        |row| row.text(0),
                    )
                    .await
                    .unwrap();
                assert_eq!(settled, "accepted");
            } else {
                assert!(failure_count(&report) > 0, "{report}");
                assert!(report.to_string().contains("debt-at-seal"), "{report}");
                assert_eq!(
                    repo.branch_head(),
                    before,
                    "the refused seal moves no bookmark"
                );
                assert_eq!(
                    repo.read("debt.txt"),
                    "before\n",
                    "the proposed patch never landed"
                );
            }
        }
    }
}

#[tokio::test]
async fn a_locked_write_adopts_three_owner_publications_without_spending_race_retries() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        return;
    };
    repo.write("notes.txt", "alpha\n");
    let id = "locked-write-owner-extensions";
    let mut request = make_request(
        repo.cwd(),
        json!({
            "changes":[{"target":"file:notes.txt", "mode":"patch",
                "payload":{"old_string":"alpha", "new_string":"alpha-mine"}}],
            "commit_msg":"write on latest owner head"
        }),
    );
    request.run_id = Some("run-change".into());
    record_write_call(&repo.orch.db.local, id, &request.payload).await;
    request.tool_use_id = Some(id.into());
    let observations =
        cairn_core::internal::mcp::handlers::write::test_pause::observe_interpretations(id);
    let (optimistic, resume_optimistic) =
        cairn_core::internal::mcp::handlers::write::test_pause::before_seal(id);
    let (locked, resume_locked) =
        cairn_core::internal::mcp::handlers::write::test_pause::before_locked_write(id);
    let orch = repo.orch.clone();
    let write = tokio::spawn(async move { handle_write(&orch, &request).await });
    tokio::time::timeout(std::time::Duration::from_secs(60), optimistic.notified())
        .await
        .unwrap();
    let publish = |text: &'static str, replacement: &'static str| {
        repo.change_report(json!({
            "changes":[{"target":"file:notes.txt", "mode":"patch",
                "payload":{"old_string":text, "new_string":replacement}}],
            "commit_msg":"extend owner history"
        }))
    };
    assert_successful_change(&publish("alpha", "alpha\nfirst").await, 1);
    resume_optimistic.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(60), locked.notified())
        .await
        .expect("the locked attempt prepared, but has not taken its writer guard");
    for (old, new) in [
        ("first", "first\nsecond"),
        ("second", "second\nthird"),
        ("third", "third\nfourth"),
    ] {
        assert_successful_change(&publish(old, new).await, 1);
    }
    let adopted = repo.branch_head();
    resume_locked.notify_one();
    let report = parse_report(&write.await.unwrap());
    assert_successful_change(&report, 1);
    assert_eq!(
        observations.locked_preparations(),
        1,
        "descendant owner commits do not consume the bounded replacement-history retries"
    );
    let interpreted = observations.coordinates();
    assert_eq!(interpreted.len(), 2);
    assert_eq!(interpreted[1].0, adopted);
    assert_eq!(
        repo.read("notes.txt"),
        "alpha-mine\nfirst\nsecond\nthird\nfourth\n"
    );
}

#[tokio::test]
async fn an_optimistic_write_restarts_when_only_its_base_coordinate_changes() {
    for move_declaration in [false, true] {
        let Some(repo) = ChangeTestRepo::try_new().await else {
            return;
        };
        repo.write("notes.txt", "alpha\n");
        let before = repo.branch_head();
        let store = jj::project_store_dir(repo.config.path(), repo.project.path());
        let orch = repo.orch.clone();
        let mut request = make_request(
            repo.cwd(),
            json!({
                "changes": [{"target":"file:notes.txt", "mode":"patch",
                    "payload":{"old_string":"alpha", "new_string":"alpha-mine"}}],
                "commit_msg":"edit against current base"
            }),
        );
        request.run_id = Some("run-change".into());
        let tool_use_id = if move_declaration {
            "base-declaration-race"
        } else {
            "base-tip-race"
        };
        record_write_call(&repo.orch.db.local, tool_use_id, &request.payload).await;
        request.tool_use_id = Some(tool_use_id.to_string());
        let interpretations =
            cairn_core::internal::mcp::handlers::write::test_pause::observe_interpretations(
                tool_use_id,
            );
        let (reached, resume) =
            cairn_core::internal::mcp::handlers::write::test_pause::before_seal(tool_use_id);
        let write = tokio::spawn(async move { handle_write(&orch, &request).await });
        tokio::time::timeout(std::time::Duration::from_secs(60), reached.notified())
            .await
            .expect("the optimistic write has captured its base and prepared its patch");
        let held = repo.orch.hold_jj_store_lock_for_test(&store).await;
        let env = jj::environment(&repo.jj_bin, repo.config.path());
        if move_declaration {
            let base = jj::bookmark_commit(&env, &store, "main").unwrap();
            env.run(
                &store,
                &[
                    "bookmark",
                    "set",
                    "alternate-base",
                    "-r",
                    &base,
                    "--ignore-working-copy",
                ],
                "create equal-tip integration target",
            )
            .unwrap();
            repo.orch
                .db
                .local
                .execute(
                    "UPDATE jobs SET base_branch = 'alternate-base' WHERE id = 'job-change'",
                    (),
                )
                .await
                .unwrap();
        } else {
            env.run(
                &store,
                &[
                    "bookmark",
                    "set",
                    "main",
                    "-r",
                    &before,
                    "--ignore-working-copy",
                ],
                "move only integration base",
            )
            .unwrap();
        }
        assert_eq!(repo.branch_head(), before, "the authored head did not move");
        let first = interpretations.coordinates();
        assert_eq!(
            first.len(),
            1,
            "the first patch interpretation has completed"
        );
        let expected_base = if move_declaration {
            "alternate-base"
        } else {
            "main"
        };
        let expected_commit = jj::bookmark_commit(&env, &store, expected_base).unwrap();
        drop(held);
        resume.notify_one();
        let report = parse_report(&write.await.unwrap());
        assert_successful_change(&report, 1);
        assert_eq!(repo.read("notes.txt"), "alpha-mine\n");
        let interpreted = interpretations.coordinates();
        assert_eq!(
            interpreted.len(),
            2,
            "a base-only race must reinterpret, not merely seal successfully"
        );
        assert_eq!(interpreted[0], first[0]);
        assert_eq!(
            interpreted[1],
            (before, expected_base.to_string(), expected_commit)
        );
        assert_ne!(
            interpreted[0], interpreted[1],
            "the restarted interpretation uses the new base identity"
        );
    }
}

/// Two deliveries of one write call, where the first seals after the second
/// read the replay ledger (without the lock) and before it read the head. The
/// second then finds `old` gone from the moved head. It must still answer with
/// the first delivery's recorded report rather than an anchor failure: the
/// replay epoch is re-checked under the store lock before any optimistic
/// outcome is returned, failures included (I5).
async fn a_duplicate_delivery_sealed_inside_the_optimistic_window_replays(atomic: bool) {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("notes.txt", "old\n");
    let tool_use_id = if atomic {
        "toolu_duplicate_atomic"
    } else {
        "toolu_duplicate"
    };
    let payload = json!({
        "changes": [{
            "target": "file:notes.txt",
            "mode": "patch",
            "payload": { "old_string": "old", "new_string": "new" }
        }],
        "commit_msg": "old to new",
        "atomic": atomic
    });
    record_write_call(&repo.orch.db.local, tool_use_id, &payload).await;

    let (reached, resume) =
        cairn_core::internal::mcp::handlers::write::test_pause::after_replay_lookup(tool_use_id);
    let orch = repo.orch.clone();
    let mut request = make_request(repo.cwd(), payload.clone());
    request.run_id = Some("run-change".to_string());
    request.tool_use_id = Some(tool_use_id.to_string());
    let second = tokio::spawn(async move { handle_write(&orch, &request).await });
    tokio::time::timeout(std::time::Duration::from_secs(60), reached.notified())
        .await
        .expect("the second delivery parks after its replay lookup");

    let first = repo.change_report_as_tool_use(tool_use_id, payload).await;
    assert_successful_change(&first, 1);
    resume.notify_one();

    let second = parse_report(&second.await.unwrap());
    assert!(
        second["replayed"]
            .as_str()
            .is_some_and(|note| note.contains("delivered more than once")),
        "answered as a replay: {second:?}"
    );
    assert_successful_change(&second, 1);
    assert_eq!(
        second["commit"], first["commit"],
        "the recorded report, not a new one"
    );
    assert_eq!(repo.read("notes.txt"), "new\n");
}

#[tokio::test]
async fn a_duplicate_atomic_delivery_sealed_inside_the_optimistic_window_replays() {
    a_duplicate_delivery_sealed_inside_the_optimistic_window_replays(true).await;
}

#[tokio::test]
async fn a_duplicate_delivery_sealed_inside_the_optimistic_window_replays_non_atomic() {
    a_duplicate_delivery_sealed_inside_the_optimistic_window_replays(false).await;
}

/// cairn/4897: a live builder's own branch was removed out from under it
/// mid-turn. Every read, write, and command then answered "Could not resolve
/// branch ref", which reads like a typo to retry. The session must instead be
/// told that its branch is gone, that it did not cause this, and that it should
/// stop. `run` resolves the same coordinate through the same function
/// (`resolve_current_for_read`), but its handler needs executor fixtures this
/// file does not have, so only read and write are driven here.
#[tokio::test]
async fn a_live_run_whose_own_branch_is_removed_gets_a_typed_refusal_on_read_and_write() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("lost.txt", "work\n");
    let before =
        handle_read_file(&repo.orch, &make_read_request(repo.cwd(), "file:lost.txt")).await;
    assert!(
        before.contains("work"),
        "the premise is a branch that resolves: {before}"
    );

    for args in [
        vec!["bookmark", "delete", "agent/CHG-1-builder-0"],
        vec!["git", "export"],
    ] {
        let output = std::process::Command::new(&repo.jj_bin)
            .args(&args)
            .current_dir(repo.dir.path())
            .output()
            .unwrap();
        assert!(output.status.success(), "jj {args:?} failed: {output:?}");
    }

    let read = handle_read_file(&repo.orch, &make_read_request(repo.cwd(), "file:lost.txt")).await;
    let write = repo
        .change_raw(json!({
            "changes": [{
                "target": "file:lost.txt",
                "mode": "patch",
                "payload": { "old_string": "work", "new_string": "more work" }
            }],
            "commit_msg": "must not land"
        }))
        .await;
    for (verb, answer) in [("read", &read), ("write", &write)] {
        assert!(
            answer.contains("own branch 'agent/CHG-1-builder-0' no longer exists"),
            "{verb} must name the lost branch: {answer}"
        );
        assert!(
            answer.contains("Stop here"),
            "{verb} must say stop: {answer}"
        );
        assert!(
            !answer.contains("Could not resolve branch ref"),
            "{verb} must not answer with the raw ref failure: {answer}"
        );
    }
}

fn make_read_request(cwd: &str, path: &str) -> McpCallbackRequest {
    McpCallbackRequest {
        answer_bound_ms: None,
        thread_id: None,
        cwd: cwd.to_string(),
        run_id: Some("run-change".to_string()),
        tool: "read".to_string(),
        payload: json!({ "path": path }),
        tool_use_id: None,
        origin: None,
    }
}

async fn count_rows(db: &LocalDb, sql: &'static str) -> i64 {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn.query(sql, ()).await?;
            let row = rows.next().await?.expect("missing count row");
            row.i64(0)
        })
    })
    .await
    .unwrap()
}

fn init_git_repo(path: &std::path::Path) {
    for args in [
        vec!["init", "-b", "main"],
        vec!["config", "user.email", "test@example.com"],
        vec!["config", "user.name", "Test User"],
        vec!["commit", "--allow-empty", "-m", "initial"],
    ] {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(path)
            .output()
            .unwrap();
        assert!(output.status.success(), "git setup failed: {output:?}");
    }
}

async fn seed_change_run(
    db: &LocalDb,
    project_id: &str,
    _worktree: &Path,
    branch: &str,
    base_commit: &str,
) {
    let mut agents = HashMap::new();
    agents.insert(
        "agent-1".to_string(),
        AgentSnapshot {
            edited_at: None,
            id: "agent-1".to_string(),
            name: "Builder".to_string(),
            description: String::new(),
            prompt: String::new(),
            tools: Vec::new(),
            models: Vec::new(),
            selection: None,
            provider_account: None,
            model: None,
            disallowed_tools: None,
            skills: None,
            fence: Some(Fence::default()),
            sandbox: None,
            on_escape: None,
            resolved_backend: None,
            extras: None,
            seed_budget_tokens: None,
            seed_eras: None,
        },
    );
    let snapshot = ExecutionSnapshot::new(
        RecipeSnapshot {
            id: "recipe-1".to_string(),
            name: "Change test".to_string(),
            description: None,
            trigger: RecipeTrigger::Manual,
            nodes: Vec::new(),
            edges: Vec::new(),
        },
        agents,
        HashMap::new(),
        TriggerContext {
            issue_id: Some("issue-change".to_string()),
            project_id: project_id.to_string(),
            trigger_type: TriggerType::Manual,
            event_payload: None,
            initiated_via: None,
        },
    )
    .to_json()
    .unwrap();
    db.write(|conn| {
        let project_id = project_id.to_string();
        let branch = branch.to_string();
        let base_commit = base_commit.to_string();
        let snapshot = snapshot.clone();
        Box::pin(async move {
            conn.execute("INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at) VALUES ('issue-change', ?1, 1, 'Change', 'active', 1, 1)", params![project_id.as_str()]).await?;
            conn.execute("INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot, triggered_by) VALUES ('exec-change', 'recipe-1', 'issue-change', ?1, 'running', 1, 1, ?2, 'manual')", params![project_id.as_str(), snapshot.as_str()]).await?;
            conn.execute("INSERT INTO jobs(id, execution_id, agent_config_id, issue_id, project_id, node_name, status, uri_segment, branch, base_commit, created_at, updated_at) VALUES ('job-change', 'exec-change', 'agent-1', 'issue-change', ?1, 'builder', 'running', 'builder', ?2, ?3, 1, 1)", params![project_id.as_str(), branch.as_str(), base_commit.as_str()]).await?;
            conn.execute("INSERT INTO sessions(id, job_id, status, created_at, updated_at) VALUES ('session-change', 'job-change', 'active', 1, 1)", ()).await?;
            conn.execute("INSERT INTO runs(id, project_id, issue_id, job_id, status, session_id, created_at, updated_at, start_mode) VALUES ('run-change', ?1, 'issue-change', 'job-change', 'live', 'session-change', 1, 1, 'resume')", params![project_id.as_str()]).await?;
            conn.execute("INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, created_at, updated_at) VALUES ('turn-change', 'session-change', 'run-change', 'job-change', 1, 'running', 1, 1)", ()).await?;
            conn.execute("UPDATE jobs SET current_session_id = 'session-change', current_turn_id = 'turn-change' WHERE id = 'job-change'", ()).await?;
            Ok(())
        })
    }).await.unwrap();
}

fn parse_report(result: &str) -> serde_json::Value {
    let mut values = serde_json::Deserializer::from_str(result).into_iter::<serde_json::Value>();
    let report = values
        .next()
        .and_then(Result::ok)
        .unwrap_or_else(|| panic!("Expected JSON ChangeReport, got: {result}"));
    let trailing = result[values.byte_offset()..].trim();
    // These fixtures normally arm no checks, but planning can outlive the
    // inline wait on a loaded host even when the wave eventually finds none.
    // Match the whole pending envelope, not just "Checks:", so unrelated text
    // or a second JSON value cannot be silently discarded.
    let pending = trailing
        .strip_prefix("Checks: ⋯ pending (")
        .and_then(|rest| {
            rest.strip_suffix(
                ") — the commit above landed; its when:write checks continue \
                 in the background. Their verdicts (and any formatter fix, which lands as its own commit) appear \
                 at cairn:~/checks, and a [Write checks] message follows when they finish.",
            )
        })
        .is_some_and(|why| {
            !why.contains(['\n', '\r'])
                && [
                    "not admitted after ",
                    "running on ",
                    "running since ",
                    "running, still running after ",
                    "admitted but not running after ",
                ]
                    .iter()
                    .any(|prefix| why.strip_prefix(prefix).is_some_and(|reason| !reason.is_empty()))
        });
    assert!(
        trailing.is_empty() || pending,
        "Unexpected text after JSON ChangeReport: {trailing}"
    );
    report
}

#[test]
fn parse_report_accepts_plain_json_and_rejects_unknown_trailing_text() {
    let report = json!({"applied": [], "failures": []});
    assert_eq!(parse_report(&format!("  {report} \n\t")), report);
    for trailing in [
        "unexpected diagnostic",
        "{}",
        "Checks: unknown summary",
        "Checks: ⋯ pending (not admitted after 0s)",
        "Checks: ⋯ pending (not admitted after 0s) — the commit above landed;",
    ] {
        assert!(
            std::panic::catch_unwind(|| parse_report(&format!("{report}\n\n{trailing}"))).is_err(),
            "must reject unrecognised trailing text: {trailing}"
        );
    }
}

#[tokio::test]
async fn parse_report_accepts_an_expired_write_check_wait() {
    let orch = test_file_change_orchestrator().await;
    let project = tempfile::tempdir().unwrap();
    common::insert_project_with_repo(&orch.db.local, "mcp", project.path()).await;
    let summary =
        cairn_core::internal::execution::checks::expire_write_check_wait_for_test(&orch).await;
    assert!(
        summary.starts_with("Checks: ⋯ pending (not admitted after "),
        "{summary}"
    );
    assert!(
        summary.ends_with("a [Write checks] message follows when they finish."),
        "{summary}"
    );

    let raw = handle_write(
        &orch,
        &make_request(
            "/tmp/test-repo",
            json!({"changes": [{
                "target": "cairn://p/mcp/messages",
                "mode": "append",
                "payload": {"content": "report with pending checks"}
            }]}),
        ),
    )
    .await;
    let expected = parse_report(&raw);
    assert_successful_change(&expected, 1);
    let with_summary = format!("{raw}\n\n{summary}");
    assert_eq!(parse_report(&with_summary), expected);
    for malformed in [
        format!("{with_summary}\nunknown diagnostic"),
        with_summary.replace("not admitted after ", "unknown state "),
        with_summary.replace("not admitted after ", "not admitted after \n"),
    ] {
        assert!(
            std::panic::catch_unwind(|| parse_report(&malformed)).is_err(),
            "must reject malformed pending footer: {malformed}"
        );
    }
}

fn failure_count(report: &serde_json::Value) -> usize {
    report["failures"].as_array().map(Vec::len).unwrap_or(0)
}

fn assert_successful_change(report: &serde_json::Value, applied_count: usize) {
    assert_eq!(failure_count(report), 0, "{report:?}");
    assert_eq!(
        report["applied"].as_array().unwrap().len(),
        applied_count,
        "{report:?}"
    );
}

struct ChangeTestRepo {
    dir: tempfile::TempDir,
    project: tempfile::TempDir,
    _origin: tempfile::TempDir,
    config: tempfile::TempDir,
    jj_bin: String,
    orch: Orchestrator,
    project_id: String,
}

impl ChangeTestRepo {
    /// Provision a jj-workspace-backed change test, or `None` to self-skip when
    /// jj is unavailable. The cwd (`dir`) is a real `.jj` workspace over a shared
    /// store backed by a throwaway project git repo.
    async fn try_new() -> Option<Self> {
        Self::try_new_on_base(|_| {}).await
    }

    /// [`Self::try_new`] over a base `prepare_base` shapes: it runs in the
    /// project repo before the workspace exists, so what it commits is on the
    /// builder's base commit and in the project's primary checkout alike.
    async fn try_new_on_base(prepare_base: impl FnOnce(&Path)) -> Option<Self> {
        let jj_bin = common::jj_bin()?;
        let dir = tempfile::tempdir().unwrap();
        let project = tempfile::Builder::new()
            .prefix("cairn-change-project-")
            .tempdir()
            .unwrap();
        init_git_repo(project.path());
        prepare_base(project.path());
        let origin = tempfile::tempdir().unwrap();
        let output = std::process::Command::new("git")
            .args(["init", "-b", "main", "--bare"])
            .current_dir(origin.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "bare origin setup failed: {output:?}"
        );
        let output = std::process::Command::new("git")
            .args(["remote", "add", "origin", origin.path().to_str().unwrap()])
            .current_dir(project.path())
            .output()
            .unwrap();
        assert!(output.status.success(), "origin setup failed: {output:?}");
        let config = tempfile::tempdir().unwrap();
        let orch = orchestrator_with_config(config.path()).await;
        let branch = "agent/CHG-1-builder-0";
        common::provision_jj_workspace(config.path(), project.path(), dir.path(), branch);
        let project_id =
            common::insert_project_with_repo(&orch.db.local, "chg", project.path()).await;
        let base_commit = common::head_sha(project.path());
        seed_change_run(
            &orch.db.local,
            &project_id,
            dir.path(),
            branch,
            &base_commit,
        )
        .await;
        Some(Self {
            dir,
            project,
            _origin: origin,
            config,
            jj_bin,
            orch,
            project_id,
        })
    }

    /// Put an open, remote pull request on this branch, which is what makes
    /// publication to origin mandatory rather than deferred.
    async fn open_remote_pr_on_branch(&self) {
        self.orch
            .db
            .local
            .execute(
                "INSERT INTO merge_requests
                 (id, job_id, project_id, title, source_branch, target_branch, status, is_local, opened_at, updated_at, github_pr_number)
                 VALUES ('mr-chg', 'job-chg', ?1, 'PR', 'agent/CHG-1-builder-0', 'main', 'open', 0, 1, 1, 4242)",
                cairn_db::turso::params![self.project_id.clone()],
            )
            .await
            .unwrap();
    }

    /// Point `origin` at nothing, so the required push fails while the commit
    /// itself still lands. The remote stays *configured*, so this is a genuine
    /// publication failure rather than the benign no-remote case.
    fn break_origin(&self) {
        let output = std::process::Command::new("git")
            .args(["remote", "set-url", "origin", "/nonexistent/origin.git"])
            .current_dir(self.project.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "breaking origin failed: {output:?}"
        );
    }

    /// Seal the current working copy into a base commit, so a later delete or
    /// edit operates against committed content rather than un-sealed `@` dirt.
    fn seal_base(&self, msg: &str) {
        let jj = jj::environment_with_binary(&self.jj_bin, self.config.path());
        jj::seal(&jj, self.dir.path(), msg, None).unwrap();
        let output = std::process::Command::new(&self.jj_bin)
            .args(["bookmark", "set", "agent/CHG-1-builder-0", "-r", "@"])
            .current_dir(self.dir.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "bookmark advance failed: {output:?}"
        );
    }

    fn cwd(&self) -> &str {
        self.dir.path().to_str().unwrap()
    }

    fn path(&self, path: &str) -> std::path::PathBuf {
        self.dir.path().join(path)
    }

    fn write(&self, path: &str, content: &str) {
        let path = self.path(path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, content).unwrap();
        let output = std::process::Command::new(&self.jj_bin)
            .args(["bookmark", "set", "agent/CHG-1-builder-0", "-r", "@"])
            .current_dir(self.dir.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "fixture snapshot failed: {output:?}"
        );
    }

    fn read(&self, path: &str) -> String {
        self.read_at("agent/CHG-1-builder-0", path)
    }

    fn read_at(&self, rev: &str, path: &str) -> String {
        let output = std::process::Command::new(&self.jj_bin)
            .args(["file", "show", "-r", rev, path])
            .current_dir(self.dir.path())
            .output()
            .unwrap();
        assert!(output.status.success(), "jj file show failed: {output:?}");
        String::from_utf8(output.stdout).unwrap()
    }

    fn logical_exists(&self, path: &str) -> bool {
        std::process::Command::new(&self.jj_bin)
            .args(["file", "show", "-r", "agent/CHG-1-builder-0", path])
            .current_dir(self.dir.path())
            .output()
            .unwrap()
            .status
            .success()
    }

    fn branch_head(&self) -> String {
        let output = std::process::Command::new(&self.jj_bin)
            .args([
                "log",
                "-r",
                "agent/CHG-1-builder-0",
                "--no-graph",
                "-T",
                "commit_id",
            ])
            .current_dir(self.dir.path())
            .output()
            .unwrap();
        assert!(output.status.success(), "jj log failed: {output:?}");
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    }

    async fn change_report(&self, payload: serde_json::Value) -> serde_json::Value {
        let mut request = make_request(self.cwd(), payload);
        request.run_id = Some("run-change".to_string());
        parse_report(&handle_write(&self.orch, &request).await)
    }

    /// Drive `handle_write` carrying an explicit `tool_use_id`, the correlation
    /// key a replayed delivery of the same tool call arrives under.
    async fn change_report_as_tool_use(
        &self,
        tool_use_id: &str,
        payload: serde_json::Value,
    ) -> serde_json::Value {
        let mut request = make_request(self.cwd(), payload);
        request.run_id = Some("run-change".to_string());
        request.tool_use_id = Some(tool_use_id.to_string());
        parse_report(&handle_write(&self.orch, &request).await)
    }

    /// `handle_write`'s raw answer. A refusal taken before any mutation applies
    /// is prose, not a change report, so it cannot go through `change_report`.
    async fn change_raw(&self, payload: serde_json::Value) -> String {
        let mut request = make_request(self.cwd(), payload);
        request.run_id = Some("run-change".to_string());
        handle_write(&self.orch, &request).await
    }

    /// Turn this fixture's issue into a thread, changing nothing else, and
    /// return the thread's id. The kind is then the only difference between the
    /// two directions asserted below.
    ///
    /// `execute_script`, not `execute`: this is several statements and `execute`
    /// runs only the first. Re-pointing the job at the thread is the statement
    /// that makes it thread-owned, so losing it left an ordinary issue-owned job
    /// the fence then correctly declined to refuse (CAIRN-3874).
    ///
    /// The run is detached from the issue before the issue is deleted, because
    /// `runs.issue_id` cascades — and because that IS a thread session's shape:
    /// a job and a run, with no issue anywhere in the chain.
    async fn become_a_thread(&self) -> String {
        self.orch
            .db
            .local
            .execute_script(
                "INSERT INTO threads(id,project_id,name,status,attention,created_at,updated_at) SELECT id,project_id,'change-thread','active','none',created_at,updated_at FROM issues WHERE id='issue-change'; UPDATE jobs SET thread_id='issue-change',issue_id=NULL,node_name='Thread',uri_segment='thread' WHERE issue_id='issue-change'; UPDATE runs SET issue_id=NULL WHERE issue_id='issue-change'; UPDATE executions SET issue_id=NULL,seq=NULL,recipe_id='host-thread-session-job' WHERE id='exec-change'; DELETE FROM issues WHERE id='issue-change'",
            )
            .await
            .unwrap();
        "issue-change".to_string()
    }

    async fn preview_report(&self, payload: serde_json::Value) -> serde_json::Value {
        parse_report(&handle_write(&self.orch, &make_preview_request(self.cwd(), payload)).await)
    }
}

/// A child issue's builder is how main gets repaired from inside Cairn when the
/// project's `.cairn/config.yaml` does not parse, the state in which every batch
/// placement is refused. A `write` reaches no executor, so it has to keep working
/// there, along both paths a committing write's checks can take: no checks armed
/// while the commit's own config is still broken, and a check that arms on the
/// repair commit and is refused because the primary checkout is still broken.
#[tokio::test]
async fn a_builder_commits_and_repairs_main_while_the_project_config_does_not_parse() {
    const UNPARSEABLE: &str = "checks: [unterminated\n";
    const REPAIRED: &str = "checks:\n  repair-lane:\n    command: \"true\"\n    when: write\n";
    // Far above the wave's own inline bound, so a write that hangs fails here
    // rather than wedging the suite. Promptness itself is pinned by the summary
    // below being a verdict rather than a pending line.
    let hang_bound = std::time::Duration::from_secs(120);

    let Some(repo) = ChangeTestRepo::try_new_on_base(|project| {
        std::fs::create_dir_all(project.join(".cairn")).unwrap();
        std::fs::write(project.join(".cairn/config.yaml"), UNPARSEABLE).unwrap();
        for args in [
            vec!["add", "."],
            vec!["commit", "-m", "main breaks its config"],
        ] {
            let output = std::process::Command::new("git")
                .args(args)
                .current_dir(project)
                .output()
                .unwrap();
            assert!(output.status.success(), "git setup failed: {output:?}");
        }
    })
    .await
    else {
        eprintln!("skipping: jj not resolvable");
        return;
    };

    // The broken file has to be what placement actually reads, or everything
    // below passes for the wrong reason.
    cairn_core::config::project_settings::load_execution_project_policy(repo.project.path())
        .expect_err("the primary checkout's config must refuse placement");
    assert_eq!(repo.read(".cairn/config.yaml"), UNPARSEABLE);

    let raw = tokio::time::timeout(
        hang_bound,
        repo.change_raw(json!({
            "changes": [{
                "target": "file:unrelated.txt",
                "mode": "create",
                "payload": { "content": "lands under a broken config\n" }
            }],
            "commit_msg": "commit while the config is broken"
        })),
    )
    .await
    .expect("a write under a broken config must not hang");
    // A commit whose own config does not parse arms no wave. Keep that
    // assertion explicit even though parse_report accepts pending checks.
    let report: serde_json::Value =
        serde_json::from_str(&raw).expect("a broken config must not append a check summary");
    assert_successful_change(&report, 1);
    assert_eq!(repo.read("unrelated.txt"), "lands under a broken config\n");
    assert_eq!(repo.read(".cairn/config.yaml"), UNPARSEABLE);

    let raw = tokio::time::timeout(
        hang_bound,
        repo.change_raw(json!({
            "changes": [{
                "target": "file:.cairn/config.yaml",
                "mode": "replace",
                "payload": { "content": REPAIRED }
            }],
            "commit_msg": "repair the config"
        })),
    )
    .await
    .expect("the repair write must not hang on placement");
    let (report, summary) = raw
        .split_once("\n\n")
        .unwrap_or_else(|| panic!("the repair commit's check must arm: {raw}"));
    assert_successful_change(&parse_report(report), 1);
    assert_eq!(repo.read(".cairn/config.yaml"), REPAIRED);
    assert!(
        summary.contains("repair-lane")
            && summary.contains("not run")
            && summary.contains(".cairn/config.yaml"),
        "the armed check must fail by naming the config, not by waiting to be placed: {summary}"
    );
}

/// The tracked-writes half of the thread posture, at the `write` door.
///
/// A thread owns no branch, so its job runs ON the base branch and a commit from
/// it publishes to the project's default branch with no pull request and no
/// review surface. Both directions run the same batch against the same fixture,
/// so the issue's kind is the only variable: an ordinary issue commits and the
/// content is really on the branch, and the thread is refused before anything
/// applies — the branch still holds exactly what the ordinary commit left there.
#[tokio::test]
async fn a_thread_cannot_commit_a_tracked_file_and_an_ordinary_issue_still_can() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("posture.txt", "before\n");

    let report = repo
        .change_report(json!({
            "changes": [{
                "target": "file:posture.txt",
                "mode": "patch",
                "payload": { "old_string": "before", "new_string": "after" }
            }],
            "commit_msg": "an ordinary issue lands its work"
        }))
        .await;
    assert_successful_change(&report, 1);
    assert_eq!(
        repo.read("posture.txt"),
        "after\n",
        "an ordinary issue's commit must reach the branch unchanged: {report:?}"
    );

    let thread_id = repo.become_a_thread().await;
    // The conversion is the whole experiment, so it is asserted rather than
    // assumed: a fixture that inserts a thread row but leaves the job
    // issue-owned tests nothing, and every assertion below would pass for the
    // wrong reason.
    assert_eq!(
        common::scalar_text_by_id(
            &repo.orch.db.local,
            "SELECT thread_id FROM jobs WHERE id = ?1",
            "job-change"
        )
        .await
        .as_deref(),
        Some(thread_id.as_str()),
        "the job under test must actually belong to the thread"
    );

    let refusal = repo
        .change_raw(json!({
            "changes": [{
                "target": "file:posture.txt",
                "mode": "patch",
                "payload": { "old_string": "after", "new_string": "from a thread" }
            }],
            "commit_msg": "should never land"
        }))
        .await;

    assert!(
        refusal.contains("thread-owned job") && refusal.contains("child issue"),
        "the thread must be told the posture and where the work belongs: {refusal}"
    );
    assert_eq!(
        repo.read("posture.txt"),
        "after\n",
        "the refusal must be taken before the mutation applies: {refusal}"
    );
}

/// The `write` tool has no `branch` key, and a stale caller's stray one is an
/// ignored unknown key. It must not get a thread past the commit refusal: naming
/// a ref is not a way to commit from a thread, the base branch included.
#[tokio::test]
async fn a_stray_branch_key_does_not_get_a_thread_past_the_commit_refusal() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("posture.txt", "before\n");
    repo.seal_base("seed stray branch specimen");
    repo.become_a_thread().await;
    let head = repo.branch_head();
    let main_head = common::head_sha(repo.project.path());

    let refusal = repo
        .change_raw(json!({
            "changes": [{
                "target": "file:posture.txt",
                "mode": "patch",
                "payload": { "old_string": "before", "new_string": "from a thread" }
            }],
            "branch": "main",
            "commit_msg": "should never land"
        }))
        .await;

    assert!(
        refusal.contains("thread-owned job") && refusal.contains("child issue"),
        "a stray branch key must not skip the refusal: {refusal}"
    );
    assert_eq!(repo.branch_head(), head, "nothing committed: {refusal}");
    assert_eq!(
        common::head_sha(repo.project.path()),
        main_head,
        "nothing reached main: {refusal}"
    );
}

/// A batch with file edits holds the store lock for its logical transaction and
/// dispatches its resource items inside that hold. Both `cairn:~/rebase` actions
/// take the same lock inline, so dispatched there they waited on their own
/// batch until the callback socket gave up (cairn/5509). The batch is refused
/// before any lock is taken. The test holds the lock itself, so a batch that
/// reached any acquisition would wait instead of answering.
#[tokio::test]
async fn a_file_batch_with_a_rebase_item_is_refused_before_taking_the_store_lock() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("rebase-batch.txt", "before\n");
    repo.seal_base("seed rebase batch specimen");
    let store = jj::project_store_dir(repo.config.path(), repo.project.path());
    let held = repo.orch.hold_jj_store_lock_for_test(&store).await;

    for (target, action) in [
        ("cairn:~/rebase", "replay"),
        (
            "cairn://p/chg/1/1/builder/rebase",
            "fast-forward-from-origin",
        ),
    ] {
        let report = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            repo.change_report(json!({
                "changes": [
                    {
                        "target": "file:rebase-batch.txt",
                        "mode": "patch",
                        "payload": { "old_string": "before", "new_string": "after" }
                    },
                    { "target": target, "mode": "patch", "payload": { "action": action } }
                ],
                "commit_msg": "edit and rebase in one batch"
            })),
        )
        .await
        .unwrap_or_else(|_| panic!("{action}: the batch waited on the store lock"));

        assert_eq!(
            report["applied"].as_array().map(Vec::len),
            Some(0),
            "{action}: {report:?}"
        );
        assert_eq!(failure_count(&report), 1, "{action}: {report:?}");
        assert_eq!(report["failures"][0]["index"], 1, "{action}: {report:?}");
        let error = report["failures"][0]["error"].as_str().unwrap();
        assert!(
            error.contains("Refusing cairn:~/rebase in a batch with file edits")
                && error.contains("as its own write"),
            "{action}: the refusal must name the rule and the remedy: {error}"
        );
    }
    assert!(
        repo.orch.jj_store_lock_waiters_for_test(&store).is_empty(),
        "a refused batch must leave nothing queued on the store lock"
    );
    drop(held);
    assert_eq!(repo.read("rebase-batch.txt"), "before\n");
}

/// A pull-request action pushes or merges through the store whose lock a file
/// batch holds, so a batch that carries one is refused before anything applies,
/// atomic or not: no file is edited, the branch does not move, and the pull
/// request is not touched.
#[tokio::test]
async fn a_file_batch_with_a_pull_request_action_is_refused_before_anything_applies() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("action-batch.txt", "before\n");
    repo.seal_base("seed pull-request action specimen");
    bind_remote_pr_to_job(&repo).await;
    let edits =
        cairn_core::pr_data::actions::prose_edit_seam::install(repo.project.path(), |_| Ok(()));
    let head = repo.branch_head();
    let store = jj::project_store_dir(repo.config.path(), repo.project.path());

    for atomic in [true, false] {
        for (target, action) in [
            ("cairn:~/create-pr", "publish"),
            ("cairn://p/chg/1/1/builder/create-pr", "merge"),
        ] {
            let report = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                repo.change_report(json!({
                    "changes": [
                        {
                            "target": "file:action-batch.txt",
                            "mode": "patch",
                            "payload": { "old_string": "before", "new_string": "after" }
                        },
                        { "target": target, "mode": "patch", "payload": { "action": action } }
                    ],
                    "commit_msg": "edit and act on the pull request in one batch",
                    "atomic": atomic
                })),
            )
            .await
            .unwrap_or_else(|_| panic!("{action} atomic={atomic}: the batch waited on a lock"));

            assert_eq!(
                report["applied"].as_array().map(Vec::len),
                Some(0),
                "{action} atomic={atomic}: {report:?}"
            );
            assert_eq!(
                failure_count(&report),
                1,
                "{action} atomic={atomic}: {report:?}"
            );
            assert_eq!(
                report["failures"][0]["index"], 1,
                "{action} atomic={atomic}"
            );
            let error = report["failures"][0]["error"].as_str().unwrap();
            assert!(
                error.contains("Refusing a pull-request action")
                    && error.contains("as its own write"),
                "{action} atomic={atomic}: the refusal must name the rule and the remedy: {error}"
            );
            assert!(
                report["commit"].is_null(),
                "{action} atomic={atomic}: {report:?}"
            );
        }
    }
    assert_eq!(repo.read("action-batch.txt"), "before\n");
    assert_eq!(repo.branch_head(), head, "the branch must not move");
    assert!(
        edits.probes().is_empty(),
        "the pull request must not be touched"
    );
    assert!(
        repo.orch.jj_store_lock_waiters_for_test(&store).is_empty(),
        "a refused batch must leave nothing queued on the store lock"
    );
}

/// The store lock is not reentrant. A task that asks for it while holding it
/// would otherwise queue behind itself forever; it must fail at once instead,
/// and its unwind must release the lock for everyone else.
#[tokio::test]
async fn a_task_re_acquiring_the_store_lock_it_holds_fails_fast_and_releases_it() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    let store = jj::project_store_dir(repo.config.path(), repo.project.path());
    let orch = repo.orch.clone();
    let task_store = store.clone();
    let reentrant = tokio::spawn(async move {
        let _outer = orch.hold_jj_store_lock_for_test(&task_store).await;
        let _inner = orch.hold_jj_store_lock_for_test(&task_store).await;
    });
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(10), reentrant)
        .await
        .expect("a same-task re-acquire must not wait on its own guard");
    assert!(outcome.unwrap_err().is_panic());

    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        repo.orch.hold_jj_store_lock_for_test(&store),
    )
    .await
    .expect("the panicking task's guard must have been released");
}

/// The refusal is keyed on the `commit_msg`, which is the whole surface a
/// tracked write needs — so a thread keeps every resource mutation it depends on
/// to do its actual job (its record, children, messages, todos). A guard that fired on
/// authorship rather than on the commit field would silence the thread.
#[tokio::test]
async fn a_thread_still_writes_resources() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.become_a_thread().await;

    // Project messages, not the converted issue's: a first-class thread has no
    // backing issue, so `cairn://p/chg/1/messages` names a row the conversion
    // deletes. The target has to be one a thread session genuinely still writes,
    // or the batch fails on a missing resource and says nothing about the guard.
    let report = repo
        .change_report(json!({
            "changes": [{
                "target": "cairn://p/chg/messages",
                "mode": "append",
                "payload": { "content": "a thread speaking on its project" }
            }]
        }))
        .await;

    assert_successful_change(&report, 1);
    assert_eq!(
        count_rows(
            &repo.orch.db.local,
            "SELECT COUNT(*) FROM messages WHERE channel_type = 'project' AND content = 'a thread speaking on its project'"
        )
        .await,
        1,
        "the mutation must reach the database, not just the report: {report:?}"
    );
}

/// The incident shape: a patch that inserts a block *before* an anchor and
/// re-emits the anchor verbatim inside `new_string`. Such a patch is not
/// idempotent — after it applies its `old_string` is still present, so a second
/// delivery matches again, inserts a second copy, and reports success.
fn anchor_preserving_insertion(commit_msg: &str) -> serde_json::Value {
    json!({
        "changes": [{
            "target": "file:contracts.rs",
            "mode": "patch",
            "payload": {
                "old_string": "pub const PROJECT_IMAGE_CONTRACT: Contract = Contract {",
                "new_string": "pub const PROJECT_IMAGES_CONTRACT: Contract = Contract {\n    name: \"images\",\n};\n\npub const PROJECT_IMAGE_CONTRACT: Contract = Contract {"
            }
        }],
        "commit_msg": commit_msg
    })
}

fn seed_bad_good_files(repo: &ChangeTestRepo) {
    repo.write("bad.rs", "old bad\n");
    repo.write("good.rs", "old good\n");
}

fn bad_good_patch_payload(commit_msg: &str, atomic: bool) -> serde_json::Value {
    let mut payload = json!({
        "changes": [
            { "target": "file:bad.rs", "mode": "patch", "payload": { "old_string": "missing", "new_string": "new bad" } },
            { "target": "file:good.rs", "mode": "patch", "payload": { "old_string": "old good", "new_string": "new good" } }
        ],
        "commit_msg": commit_msg
    });
    if atomic {
        payload["atomic"] = json!(true);
    }
    payload
}

async fn test_file_change_orchestrator() -> Orchestrator {
    let temp = tempfile::tempdir().unwrap();
    orchestrator_with_config(&temp.keep()).await
}

async fn orchestrator_with_config(config_dir: &Path) -> Orchestrator {
    let (_db_temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let search_index = Arc::new(SearchIndex::open_or_create(config_dir.join("search")).unwrap());
    let db_state = Arc::new(DbState::new(db, search_index));
    let services = Arc::new(TestServicesBuilder::new().with_git(RealGitClient).build());
    Orchestrator::builder(db_state, services, config_dir.to_path_buf()).build()
}

async fn insert_issue(db: &LocalDb, project_id: &str, number: i64, title: &str) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let project_id = project_id.to_string();
    let title = title.to_string();
    db.execute(
        "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, 'active', 1, 1)",
        params![id.as_str(), project_id.as_str(), number, title.as_str()],
    )
    .await
    .unwrap();
    id
}

#[tokio::test]
async fn change_unified_patch_adds_file_with_codex_envelope() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };

    let report = repo
        .change_report(
        json!({
            "changes": [{
                "target": "file:",
                "mode": "unified_patch",
                "payload": { "patch": "*** Begin Patch\n*** Add File: src/new.rs\n+pub fn new() {}\n*** End Patch\n" }
            }],
            "commit_msg": "add file"
        }),
    )
    .await;

    assert_successful_change(&report, 1);
    assert_eq!(
        report["applied"][0]["mode"].as_str().unwrap(),
        "unified_patch"
    );
    assert_eq!(repo.read("src/new.rs"), "pub fn new() {}");
}

/// A file body carrying a real conflict, as an agent mid-resolution would send
/// it if it forgot to finish.
const MARKER_BODY: &str =
    "fn main() {\n<<<<<<< HEAD\n    ours();\n=======\n    theirs();\n>>>>>>> main\n}\n";

/// The durable boundary CAIRN-3197 now guards at the ordinary write carrier:
/// content containing conflict markers cannot become a commit. The refusal is
/// whole-batch — the marker file is not published, and neither is its clean
/// sibling, because half a resolution is not a landable state.
#[tokio::test]
async fn change_refuses_to_commit_content_carrying_conflict_markers() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };

    let report = repo
        .change_report(json!({
            "changes": [
                { "target": "file:src/main.rs", "mode": "create", "payload": { "content": MARKER_BODY } },
                { "target": "file:src/other.rs", "mode": "create", "payload": { "content": "fn other() {}\n" } }
            ],
            "commit_msg": "resolve conflict"
        }))
        .await;

    assert_eq!(failure_count(&report), 1, "{report:?}");
    let error = report["failures"][0]["error"].as_str().unwrap();
    assert!(error.contains("conflict markers"), "{error}");
    assert!(
        error.contains("src/main.rs") && error.contains("2:<<<<<<<"),
        "the refusal names the file and the line: {error}"
    );
    assert!(
        error.contains("conflict_markers_reason"),
        "the refusal names its escape: {error}"
    );
    assert_eq!(
        report["commit"]["status"].as_str().unwrap(),
        "failed",
        "{report:?}"
    );
}

/// The false-positive case the guard must not punish: literal markers in a
/// document, committed on an explicit written reason. The default is refusal, so
/// the same batch that fails above succeeds here purely because it explained
/// itself.
#[tokio::test]
async fn change_commits_literal_markers_with_an_explicit_reason() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };

    let report = repo
        .change_report(json!({
            "changes": [
                { "target": "file:docs/conflicts.md", "mode": "create", "payload": { "content": MARKER_BODY } }
            ],
            "commit_msg": "document marker syntax",
            "conflict_markers_reason": "documenting Git conflict-marker syntax"
        }))
        .await;

    assert_successful_change(&report, 1);
    assert_eq!(
        repo.read("docs/conflicts.md").trim_end(),
        MARKER_BODY.trim_end()
    );
}

/// An all-whitespace reason is no reason at all. The escape is audited, so it
/// has to actually say something; anything else is a silent boolean bypass
/// wearing a string's clothes.
#[tokio::test]
async fn change_treats_a_blank_marker_reason_as_no_reason() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };

    let report = repo
        .change_report(json!({
            "changes": [
                { "target": "file:a.rs", "mode": "create", "payload": { "content": MARKER_BODY } }
            ],
            "commit_msg": "sneak it through",
            "conflict_markers_reason": "   "
        }))
        .await;

    assert_eq!(failure_count(&report), 1, "{report:?}");
    assert!(report["failures"][0]["error"]
        .as_str()
        .unwrap()
        .contains("conflict markers"));
}

#[tokio::test]
async fn change_unified_patch_requires_commit_msg_for_file_targets() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path().to_str().unwrap();
    let orch = test_file_change_orchestrator().await;

    let request = make_request(
        cwd,
        json!({
            "changes": [{
                "target": "file:",
                "mode": "unified_patch",
                "payload": { "patch": "*** Begin Patch\n*** Add File: new.rs\n+new();\n*** End Patch\n" }
            }]
        }),
    );

    let report = parse_report(&handle_write(&orch, &request).await);

    assert_eq!(failure_count(&report), 1, "{report:?}");
    assert!(report["failures"][0]["error"]
        .as_str()
        .unwrap()
        .contains("commit_msg"));
    assert!(!dir.path().join("new.rs").exists());
}

#[tokio::test]
async fn change_mixed_unified_patch_and_resource_batch_succeeds() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    let mut request = make_request(
        repo.cwd(),
        json!({
            "changes": [
                {
                    "target": "file:",
                    "mode": "unified_patch",
                    "payload": { "patch": "*** Begin Patch\n*** Add File: mixed.rs\n+mixed();\n*** End Patch\n" }
                },
                {
                    "target": "cairn://p/chg/messages",
                    "mode": "append",
                    "payload": { "content": "Unified patch landed" }
                }
            ],
            "commit_msg": "mixed batch"
        }),
    );
    request.run_id = Some("run-change".to_string());

    let report = parse_report(&handle_write(&repo.orch, &request).await);

    assert_eq!(report["applied"].as_array().unwrap().len(), 2);
    assert_eq!(failure_count(&report), 0, "{report:?}");
    assert_eq!(repo.read("mixed.rs"), "mixed();");
    assert_eq!(
        count_rows(
            &repo.orch.db.local,
            "SELECT COUNT(*) FROM messages WHERE channel_type = 'project' AND content = 'Unified patch landed'"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn change_atomic_promote_amend_rolls_back_decision() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("canon.md", "old canon\n");
    repo.seal_base("initial canon");
    let project_id = common::create_project(&repo.orch.db.local, "mcp").await;
    repo.orch
        .db
        .local
        .execute_script(format!(
            "\
            INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)\
             VALUES ('issue-atomic-promote', '{project_id}', 1, 'Promote memory', 'active', 1, 1);\
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)\
             VALUES ('exec-atomic-promote', 'recipe', 'issue-atomic-promote', '{project_id}', 'running', 1, 1);\
            INSERT INTO jobs(id, execution_id, issue_id, project_id, recipe_node_id, node_name, uri_segment, status, created_at, updated_at)\
             VALUES ('job-atomic-promote', 'exec-atomic-promote', 'issue-atomic-promote', '{project_id}', 'builder', 'builder', 'builder', 'running', 1, 1);\
            INSERT INTO memories (id, name, project_id, content, status, scope, scope_value, job_id, node_seq, created_at, updated_at)\
             VALUES ('atomic-promote-memory', 'atomic promote', '{project_id}', 'promote me', 'claimed', 'project', '{project_id}', 'job-atomic-promote', 1, 1, 1);"
        ))
        .await
        .unwrap();

    let report = repo
        .change_report(json!({
            "changes": [
                {
                    "target": "file:canon.md",
                    "mode": "patch",
                    "payload": { "old_string": "old canon", "new_string": "new canon" }
                },
                {
                    "target": "cairn://p/mcp/1/1/builder/memories/1",
                    "mode": "patch",
                    "payload": { "action": "promote", "reason": "ready for canon" }
                }
            ],
            "commit_msg": "^",
            "atomic": true
        }))
        .await;

    assert_eq!(failure_count(&report), 1, "{report:?}");
    assert_eq!(report["transactional"], true);
    let error = report["failures"][0]["error"].as_str().unwrap();
    assert!(
        error.contains("promote_memory cannot ride an amend"),
        "{error}"
    );

    let (status, triage_decision, reason, promoted_commit_sha) = repo
        .orch
        .db
        .local
        .query_one(
            "SELECT status, triage_decision, reason, promoted_commit_sha FROM memories WHERE id = 'atomic-promote-memory'",
            (),
            |row| {
                Ok((
                    row.text(0)?,
                    row.opt_text(1)?,
                    row.opt_text(2)?,
                    row.opt_text(3)?,
                ))
            },
        )
        .await
        .unwrap();
    assert_eq!(status, "claimed");
    assert!(triage_decision.is_none());
    assert!(reason.is_none());
    assert!(promoted_commit_sha.is_none());
}

#[tokio::test]
async fn node_memory_append_uses_target_node_and_scope_owner() {
    let orch = test_file_change_orchestrator().await;
    orch.db
        .local
        .execute(
            "INSERT OR IGNORE INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at, is_workspace)
             VALUES ('workspace', 'default', 'Workspace', 'WS', '/tmp/workspace', 1, 1, 1)",
            (),
        )
        .await
        .unwrap();
    let project_id = common::create_project(&orch.db.local, "mcp").await;
    let issue_id = insert_issue(&orch.db.local, &project_id, 1, "Memory target").await;
    orch.db
        .local
        .execute_script(format!(
            "\
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)\
             VALUES ('exec-memory-target', 'recipe', '{issue_id}', '{project_id}', 'running', 1, 1);\
            INSERT INTO jobs(id, execution_id, issue_id, project_id, recipe_node_id, node_name, uri_segment, status, created_at, updated_at)\
             VALUES ('job-builder', 'exec-memory-target', '{issue_id}', '{project_id}', 'builder', 'builder', 'builder', 'running', 1, 1);\
            INSERT INTO jobs(id, execution_id, issue_id, project_id, recipe_node_id, node_name, uri_segment, status, created_at, updated_at)\
             VALUES ('job-reviewer', 'exec-memory-target', '{issue_id}', '{project_id}', 'reviewer', 'reviewer', 'reviewer', 'running', 1, 1);"
        ))
        .await
        .unwrap();

    let request = make_request(
        "/tmp/test-repo",
        json!({
            "changes": [{
                "target": "cairn://p/mcp/1/1/reviewer/memories",
                "mode": "append",
                "payload": {
                    "name": "workspace fact",
                    "content": "Workspace-scoped memory should live with workspace canon",
                    "scope": "workspace"
                }
            }]
        }),
    );

    let report = parse_report(&handle_write(&orch, &request).await);
    assert_successful_change(&report, 1);
    let summary = report["applied"][0]["summary"].as_str().unwrap();
    assert!(
        summary.contains("cairn://p/mcp/1/1/reviewer/memories/1"),
        "{summary}"
    );
    let resolved = memory_db::resolve_node_memory_id(&orch.db.local, "mcp", 1, 1, "reviewer", 1)
        .await
        .unwrap();
    let memory_id = resolved.expect("returned node URI resolves");
    let (job_id, project_id, scope, scope_value) = orch
        .db
        .local
        .query_one(
            "SELECT job_id, project_id, scope, scope_value FROM memories WHERE id = ?1",
            params![memory_id.as_str()],
            |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?, row.text(3)?)),
        )
        .await
        .unwrap();
    assert_eq!(job_id, "job-reviewer");
    assert_eq!(project_id, "workspace");
    assert_eq!(scope, "workspace");
    assert_eq!(scope_value, "workspace");
}

#[tokio::test]
async fn change_default_non_atomic_resource_batch_continues_after_failure() {
    let orch = test_file_change_orchestrator().await;
    common::create_project(&orch.db.local, "mcp").await;

    let request = make_request(
        "/tmp/test-repo",
        json!({
            "changes": [
                { "target": "cairn://p/missing/messages", "mode": "append", "payload": { "content": "Lost" } },
                { "target": "cairn://p/mcp/messages", "mode": "append", "payload": { "content": "Applied after failure" } }
            ]
        }),
    );

    let report = parse_report(&handle_write(&orch, &request).await);

    assert_eq!(report["applied"].as_array().unwrap().len(), 1);
    assert_eq!(report["applied"][0]["index"], 1);
    assert_eq!(failure_count(&report), 1, "{report:?}");
    assert_eq!(report["failures"][0]["index"], 0);
    assert_eq!(
        count_rows(
            &orch.db.local,
            "SELECT COUNT(*) FROM messages WHERE channel_type = 'project' AND channel_id = 'mcp' AND content = 'Applied after failure'"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn change_default_non_atomic_failures_are_reported_with_blocking_appends() {
    let orch = test_file_change_orchestrator().await;

    let request = make_request(
        "/tmp/test-repo",
        json!({
            "changes": [
                { "target": "cairn://p/missing/messages", "mode": "append", "payload": { "content": "Lost" } },
                { "target": "cairn://p/mcp/1/1/builder/tasks", "mode": "append", "payload": { "subagentType": "Explore", "description": "noop", "prompt": "noop" } }
            ]
        }),
    );

    let result = handle_write(&orch, &request).await;
    let report = parse_report(result.split("\n\n").next().unwrap());

    // The request carries no run, so the task append spawns nothing: that is
    // the append's own failure, reported beside the resource failure.
    assert_eq!(report["applied"].as_array().unwrap().len(), 0);
    assert_eq!(failure_count(&report), 2, "{report:?}");
    assert_eq!(report["failures"][0]["index"], 0);
    assert!(result.contains("No project found with key 'missing'"));
    assert_eq!(report["failures"][1]["index"], 1);
    assert!(report["failures"][1]["error"]
        .as_str()
        .unwrap()
        .contains("authenticated agent request is missing its run ID"));
    assert!(report.get("blocking").is_none(), "{report:?}");
}

#[tokio::test]
async fn change_rejects_file_target_without_commit_msg() {
    // A file-target change with no commit_msg must be rejected before anything is
    // written — uncommitted worktree edits are lost if the worktree is cleaned up.
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path().to_str().unwrap();
    let orch = test_file_change_orchestrator().await;

    let request = make_request(
        cwd,
        json!({
            "changes": [{
                "target": "file:new.rs",
                "mode": "create",
                "content": "let x = 1;\n"
            }]
        }),
    );

    let result = handle_write(&orch, &request).await;
    let report = parse_report(&result);

    assert_eq!(failure_count(&report), 1, "{report:?}");
    let error = report["failures"][0]["error"].as_str().unwrap();
    assert!(error.contains("commit_msg"), "{error}");
    assert_eq!(report["applied"].as_array().unwrap().len(), 0);
    // Nothing should have been written to disk.
    assert!(!dir.path().join("new.rs").exists());
}

#[tokio::test]
async fn change_allows_resource_only_batch_without_commit_msg() {
    // Resource-only batches never touch git, so they must still work without a
    // commit_msg.
    let orch = test_file_change_orchestrator().await;
    let project_id = common::create_project(&orch.db.local, "mcp").await;
    common::seed_authenticated_thread_run(
        &orch.db.local,
        &project_id,
        "fixture",
        "run-resource-write",
    )
    .await;

    let request = make_request_as_run(
        "/tmp/test-repo",
        json!({
            "changes": [{
                "target": "cairn://p/mcp/issues",
                "mode": "append",
                "payload": { "title": "No commit msg needed" }
            }]
        }),
        Some("run-resource-write"),
    );

    let result = handle_write(&orch, &request).await;
    let report = parse_report(&result);

    assert_eq!(failure_count(&report), 0, "{report:?}");
    assert_eq!(report["applied"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn change_creates_issue_resource_without_nested_runtime_panic() {
    let orch = test_file_change_orchestrator().await;
    let project_id = common::create_project(&orch.db.local, "mcp").await;
    common::seed_authenticated_thread_run(
        &orch.db.local,
        &project_id,
        "fixture",
        "run-resource-write",
    )
    .await;

    let request = make_request_as_run(
        "/tmp/test-repo",
        json!({
            "changes": [{
                "target": "cairn://p/mcp/issues",
                "mode": "append",
                "payload": {
                    "title": "Created through change",
                    "description": "Created by regression test"
                }
            }]
        }),
        Some("run-resource-write"),
    );

    let result = handle_write(&orch, &request).await;
    let report = parse_report(&result);

    assert_eq!(failure_count(&report), 0, "{report:?}");
    assert_eq!(report["applied"].as_array().unwrap().len(), 1, "{report:?}");
    assert!(report["applied"][0]["summary"]
        .as_str()
        .unwrap()
        .contains("Created issue mcp/"));
    assert_eq!(
        count_rows(
            &orch.db.local,
            "SELECT COUNT(*) FROM issues WHERE title = 'Created through change'"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn change_appends_issue_comment_without_nested_runtime_panic() {
    let orch = test_file_change_orchestrator().await;
    let project_id = common::create_project(&orch.db.local, "mcp").await;
    insert_issue(&orch.db.local, &project_id, 1, "Comment target").await;

    let request = make_request(
        "/tmp/test-repo",
        json!({
            "changes": [{
                "target": "cairn://p/mcp/1",
                "mode": "append",
                "payload": { "content": "Regression comment" }
            }]
        }),
    );

    let result = handle_write(&orch, &request).await;
    let report = parse_report(&result);

    assert_eq!(failure_count(&report), 0, "{report:?}");
    assert_eq!(report["applied"].as_array().unwrap().len(), 1, "{report:?}");
    assert_eq!(
        report["applied"][0]["summary"].as_str().unwrap(),
        "Appended comment to issue mcp/1"
    );
    assert_eq!(
        count_rows(
            &orch.db.local,
            "SELECT COUNT(*) FROM comments c JOIN issues i ON c.issue_id = i.id WHERE i.number = 1 AND c.content = 'Regression comment'"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn change_appends_project_and_issue_messages_without_nested_runtime_panic() {
    let orch = test_file_change_orchestrator().await;
    let project_id = common::create_project(&orch.db.local, "mcp").await;
    insert_issue(&orch.db.local, &project_id, 1, "Message target").await;

    let project_request = make_request(
        "/tmp/test-repo",
        json!({
            "changes": [{
                "target": "cairn://p/mcp/messages",
                "mode": "append",
                "payload": { "content": "Project regression message" }
            }]
        }),
    );
    let issue_request = make_request(
        "/tmp/test-repo",
        json!({
            "changes": [{
                "target": "cairn://p/mcp/1/messages",
                "mode": "append",
                "payload": { "content": "Issue regression message" }
            }]
        }),
    );

    let project_report = parse_report(&handle_write(&orch, &project_request).await);
    let issue_report = parse_report(&handle_write(&orch, &issue_request).await);

    assert_eq!(failure_count(&project_report), 0, "{project_report:?}");
    assert_eq!(failure_count(&issue_report), 0, "{issue_report:?}");
    assert_eq!(
        count_rows(
            &orch.db.local,
            "SELECT COUNT(*) FROM messages WHERE channel_type = 'project' AND channel_id = 'mcp' AND content = 'Project regression message'"
        )
        .await,
        1
    );
    assert_eq!(
        count_rows(
            &orch.db.local,
            "SELECT COUNT(*) FROM messages WHERE channel_type = 'issue' AND channel_id = 'mcp/1' AND content = 'Issue regression message'"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn change_patches_issue_resource_without_nested_runtime_panic() {
    let orch = test_file_change_orchestrator().await;
    let project_id = common::create_project(&orch.db.local, "mcp").await;
    insert_issue(&orch.db.local, &project_id, 1, "Patch target").await;

    let request = make_request(
        "/tmp/test-repo",
        json!({
            "changes": [{
                "target": "cairn://p/mcp/1",
                "mode": "patch",
                "payload": { "title": "Patched through change" }
            }]
        }),
    );

    let result = handle_write(&orch, &request).await;
    let report = parse_report(&result);

    assert_eq!(failure_count(&report), 0, "{report:?}");
    assert_eq!(report["applied"].as_array().unwrap().len(), 1, "{report:?}");
    assert_eq!(
        report["applied"][0]["summary"].as_str().unwrap(),
        "Patched issue mcp/1"
    );
    assert_eq!(
        count_rows(
            &orch.db.local,
            "SELECT COUNT(*) FROM issues WHERE number = 1 AND title = 'Patched through change'"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn change_atomic_true_preserves_file_group_fail_fast() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    seed_bad_good_files(&repo);

    let report = repo
        .change_report(bad_good_patch_payload("atomic fail fast", true))
        .await;

    assert_eq!(report["applied"].as_array().unwrap().len(), 0);
    assert_eq!(failure_count(&report), 1, "{report:?}");
    assert_eq!(report["transactional"], true);
    assert_eq!(repo.read("bad.rs"), "old bad\n");
    assert_eq!(repo.read("good.rs"), "old good\n");
}

#[tokio::test]
async fn change_patches_file_with_codex_patch_envelope() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("lib.rs", "let x = 1;\n");

    let report = repo
        .change_report(json!({
            "changes": [{
                "target": "file:lib.rs",
                "mode": "patch",
                "payload": {
                    "diff": "*** Begin Patch
*** Update File: file:lib.rs
@@ -1,1 +1,1 @@
-let x = 1;
+let x = 2;
*** End Patch
"
                }
            }],
            "commit_msg": "apply codex patch"
        }))
        .await;

    assert_successful_change(&report, 1);
    assert_eq!(report["commit"]["status"], "committed", "{report:?}");
    assert_eq!(repo.read("lib.rs"), "let x = 2;\n");
}

#[tokio::test]
async fn change_default_non_atomic_applies_matching_file_items() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    seed_bad_good_files(&repo);

    let report = repo
        .change_report(bad_good_patch_payload("apply matching only", false))
        .await;

    assert_eq!(report["applied"].as_array().unwrap().len(), 1);
    assert_eq!(report["applied"][0]["index"], 1);
    assert_eq!(failure_count(&report), 1, "{report:?}");
    assert_eq!(report["failures"][0]["index"], 0);
    assert_eq!(report["partial_success"], true);
    // The successful item still commits even though a sibling failed.
    assert_eq!(report["commit"]["status"], "committed", "{report:?}");
    assert_eq!(repo.read("bad.rs"), "old bad\n");
    assert_eq!(repo.read("good.rs"), "new good\n");
}

#[tokio::test]
async fn change_default_non_atomic_commits_only_applied_files() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    seed_bad_good_files(&repo);

    let report = repo
        .change_report(bad_good_patch_payload("commit good only", false))
        .await;

    assert_eq!(report["commit"]["status"], "committed");
    assert_eq!(failure_count(&report), 1, "{report:?}");
    // Only the applied file is sealed; the failed sibling is unchanged on disk
    // (and so absent from the commit).
    assert_eq!(repo.read("good.rs"), "new good\n");
    assert_eq!(repo.read("bad.rs"), "old bad\n");
}

#[tokio::test]
async fn change_rejects_malformed_diff_before_apply() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("lib.rs", "let x = 1;\n");

    let report = repo
        .change_report(json!({
            "changes": [{
                "target": "file:lib.rs",
                "mode": "patch",
                "payload": {
                    "diff": "*** Begin Patch\n*** Update File: file:lib.rs\n-let x = 1;\n+let x = 2;\n*** End Patch\n"
                }
            }],
            "commit_msg": "malformed diff"
        }))
        .await;

    assert_eq!(failure_count(&report), 1);
    let error = report["failures"][0]["error"].as_str().unwrap();
    assert!(error.contains("Invalid diff"));
    assert!(error.contains("unified diff") || error.contains("single-file envelope"));
    assert!(!error.contains("No hunks found in diff"));
}

#[tokio::test]
async fn change_default_non_atomic_chained_file_items_compose() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("other.rs", "old other\n");

    let report = repo
        .change_report(
        json!({
            "changes": [
                { "target": "file:chain.rs", "mode": "create", "payload": { "content": "first\n" } },
                { "target": "file:chain.rs", "mode": "patch", "payload": { "old_string": "first", "new_string": "second" } },
                { "target": "file:other.rs", "mode": "patch", "payload": { "old_string": "missing", "new_string": "new other" } }
            ],
            "commit_msg": "chain compose"
        }),
    )
    .await;

    assert_eq!(report["applied"].as_array().unwrap().len(), 2);
    assert_eq!(failure_count(&report), 1, "{report:?}");
    assert_eq!(repo.read("chain.rs"), "second\n");
    assert_eq!(repo.read("other.rs"), "old other\n");
}

#[tokio::test]
async fn change_unified_patch_applies_native_contextual_hunk() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("lib.rs", "fn example() {\n    old();\n}\n\nfn other() {}\n");

    let report = repo
        .change_report(
        json!({
            "changes": [{
                "target": "file:lib.rs",
                "mode": "unified_patch",
                "payload": { "patch": "*** Begin Patch\n*** Update File: lib.rs\n@@ fn example() {\n fn example() {\n-    old();\n+    new();\n }\n*** End Patch\n" }
            }],
            "commit_msg": "native hunk"
        }),
    )
    .await;

    assert_successful_change(&report, 1);
    assert_eq!(
        repo.read("lib.rs"),
        "fn example() {\n    new();\n}\n\nfn other() {}\n"
    );
}

#[tokio::test]
async fn change_unified_patch_accepts_bare_native_header() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write(
        "README.md",
        "- [Algolia Autocomplete](https://www.algolia.com/doc/ui-libraries/autocomplete/introduction/what-is-autocomplete/) - the official Algolia Autocomplete documentation\n- [FlexSearch](https://github.com/nextapps-de/flexsearch) - the official FlexSearch documentation\n- [Zustand](https://docs.pmnd.rs/zustand/getting-started/introduction) - the official Zustand documentation\n",
    );

    let report = repo
        .change_report(
        json!({
            "changes": [{
                "target": "file:",
                "mode": "unified_patch",
                "payload": { "patch": "*** Begin Patch\n*** Update File: README.md\n@@\n - [Algolia Autocomplete](https://www.algolia.com/doc/ui-libraries/autocomplete/introduction/what-is-autocomplete/) - the official Algolia Autocomplete documentation\n - [FlexSearch](https://github.com/nextapps-de/flexsearch) - the official FlexSearch documentation\n-- [Zustand](https://docs.pmnd.rs/zustand/getting-started/introduction) - the official Zustand documentation\n+- [Zustand](https://docs.pmnd.rs/zustand/getting-started/introduction) - the official Zustand documentation\n+\n+## Unified Patch Test\n+\n+Temporary change to validate Cairn `change` with unified patch style in this throwaway worktree.\n*** End Patch" }
            }],
            "commit_msg": "bare native header"
        }),
    )
    .await;

    assert_successful_change(&report, 1);
    assert_eq!(report["commit"]["status"], "committed", "{report:?}");
    assert!(repo.read("README.md").contains("## Unified Patch Test"));
}

#[tokio::test]
async fn change_unified_patch_applies_multi_file_envelope() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("lib.rs", "old();\n");
    repo.write("delete.rs", "delete();\n");

    let report = repo
        .change_report(
        json!({
            "changes": [{
                "target": "file:",
                "mode": "unified_patch",
                "payload": { "patch": "*** Begin Patch\n*** Add File: add.rs\n+add();\n*** Update File: lib.rs\n@@ -1,1 +1,1 @@\n-old();\n+new();\n*** Delete File: delete.rs\n*** End Patch\n" }
            }],
            "commit_msg": "multi-file envelope"
        }),
    )
    .await;

    assert_successful_change(&report, 3);
    assert_eq!(repo.read("add.rs"), "add();");
    assert_eq!(repo.read("lib.rs"), "new();\n");
    assert!(!repo.logical_exists("delete.rs"));
}

#[tokio::test]
async fn change_unified_patch_native_context_header_scopes_repeated_old_lines() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write(
        "lib.rs",
        "fn first() {\n    old();\n}\n\nfn second() {\n    old();\n}\n",
    );

    let report = repo
        .change_report(
        json!({
            "changes": [{
                "target": "file:lib.rs",
                "mode": "unified_patch",
                "payload": { "patch": "*** Begin Patch\n*** Update File: lib.rs\n@@ fn second() {\n-    old();\n+    new();\n*** End Patch\n" }
            }],
            "commit_msg": "scoped hunk"
        }),
    )
    .await;

    assert_successful_change(&report, 1);
    assert_eq!(
        repo.read("lib.rs"),
        "fn first() {\n    old();\n}\n\nfn second() {\n    new();\n}\n"
    );
}

#[tokio::test]
async fn change_unified_patch_deletes_file_with_codex_envelope() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("old.rs", "old();\n");
    // Seal old.rs into the base so deleting it is a real, committable change.
    repo.seal_base("seed old.rs");

    let report = repo
        .change_report(json!({
            "changes": [{
                "target": "file:old.rs",
                "mode": "unified_patch",
                "payload": { "patch": "*** Begin Patch\n*** Delete File: old.rs\n*** End Patch\n" }
            }],
            "commit_msg": "delete file"
        }))
        .await;

    assert_successful_change(&report, 1);
    assert!(!repo.logical_exists("old.rs"));
}

#[tokio::test]
async fn change_unified_patch_updates_file_with_codex_envelope() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("lib.rs", "let x = 1;\n");

    let report = repo
        .change_report(
        json!({
            "changes": [{
                "target": "file:lib.rs",
                "mode": "unified_patch",
                "payload": { "patch": "*** Begin Patch\n*** Update File: lib.rs\n@@ -1,1 +1,1 @@\n-let x = 1;\n+let x = 2;\n*** End Patch\n" }
            }],
            "commit_msg": "update file"
        }),
    )
    .await;

    assert_successful_change(&report, 1);
    assert_eq!(repo.read("lib.rs"), "let x = 2;\n");
}

// ---------------------------------------------------------------------------
// Write replay (the edit-echo)
//
// A single `handle_write` cannot double-apply: it snapshots the logical head,
// computes whole-file replacement content, and publishes under a compare-and-swap
// inside one store-lock epoch. The echo comes from a *second delivery* of the
// same tool call — a socket that fired while the host went on to land the
// commit, a provider-level retry, anything that re-issues the payload. These
// tests pin the guard that makes the second delivery a no-op.
// ---------------------------------------------------------------------------

/// PHASE 1 REPRODUCTION, now the guard's headline test. Two byte-identical
/// deliveries of one anchor-preserving insertion under the same `tool_use_id`
/// apply once. Without the guard the second delivery matches the re-emitted
/// anchor and inserts a second copy, which is exactly the CAIRN-3242 incident.
#[tokio::test]
async fn an_anchor_preserving_insertion_is_not_applied_twice_by_a_replay() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write(
        "contracts.rs",
        "pub const PROJECT_IMAGE_CONTRACT: Contract = Contract {\n    name: \"image\",\n};\n",
    );

    let first = repo
        .change_report_as_tool_use(
            "toolu_echo",
            anchor_preserving_insertion("add images contract"),
        )
        .await;
    assert_successful_change(&first, 1);

    let replay = repo
        .change_report_as_tool_use(
            "toolu_echo",
            anchor_preserving_insertion("add images contract"),
        )
        .await;
    assert_successful_change(&replay, 1);

    let landed = repo.read("contracts.rs");
    assert_eq!(
        landed.matches("PROJECT_IMAGES_CONTRACT").count(),
        1,
        "a replayed insertion must land one copy, not two: {landed}"
    );
}

/// The replay returns the first delivery's report verbatim, so the agent that
/// re-issued the call sees the same commit it already acted on rather than a
/// second sha for a commit that never happened.
#[tokio::test]
async fn a_replayed_write_applies_once_and_returns_the_first_report() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("notes.txt", "tail\n");

    let payload = json!({
        "changes": [{
            "target": "file:notes.txt",
            "mode": "patch",
            "payload": { "old_string": "tail", "new_string": "head\ntail" }
        }],
        "commit_msg": "prepend head"
    });

    let first = repo
        .change_report_as_tool_use("toolu_same", payload.clone())
        .await;
    assert_successful_change(&first, 1);
    let replay = repo.change_report_as_tool_use("toolu_same", payload).await;

    assert_eq!(
        replay["commit"]["sha"], first["commit"]["sha"],
        "the replay must report the commit the first delivery landed: {replay:?}"
    );
    assert_eq!(repo.read("notes.txt"), "head\ntail\n");
}

#[tokio::test]
async fn a_sealed_write_replays_without_preparing_its_now_missing_base_or_waiting_for_the_store() {
    for atomic in [false, true] {
        let Some(repo) = ChangeTestRepo::try_new().await else {
            eprintln!("skipping: jj not resolvable");
            return;
        };
        repo.write("replay-coordinate.txt", "tail\n");
        let payload = json!({
            "changes": [{
                "target": "file:replay-coordinate.txt",
                "mode": "patch",
                "payload": { "old_string": "tail", "new_string": "head\ntail" }
            }],
            "atomic": atomic,
            "commit_msg": "seal the original delivery"
        });
        let first = repo
            .change_report_as_tool_use("toolu_missing_base_replay", payload.clone())
            .await;
        assert_successful_change(&first, 1);
        let head = repo.branch_head();
        repo.orch
            .db
            .local
            .execute(
                "UPDATE jobs SET base_branch = 'no-such-integration-target' WHERE id = 'job-chg'",
                (),
            )
            .await
            .unwrap();
        let store = jj::project_store_dir(repo.config.path(), repo.project.path());
        let held = repo.orch.hold_jj_store_lock_for_test(&store).await;
        let replay = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            repo.change_report_as_tool_use("toolu_missing_base_replay", payload),
        )
        .await
        .expect("a sealed replay neither prepares nor waits for a store writer");
        drop(held);
        assert_successful_change(&replay, 1);
        assert!(replay["replayed"].is_string(), "{replay:?}");
        assert_eq!(replay["commit"]["sha"], first["commit"]["sha"]);
        assert_eq!(repo.branch_head(), head);
        assert_eq!(repo.read("replay-coordinate.txt"), "head\ntail\n");
    }
}

/// The replay ledger claims its row inside the store lock, BEFORE the
/// publication ladder's remaining rungs have run. So the report it records says
/// `committed` even when the export or the required origin push then fails and
/// the call actually answers `sealed locally; unpublished`.
///
/// A redelivery must not be handed that stale success. The row is deliberately
/// immutable to competing deliveries, so if the claiming delivery does not
/// correct it, nothing ever will, and the agent is told a head nobody outside
/// Cairn can see has been published — the exact inversion the fail-closed
/// publication contract exists to prevent.
#[tokio::test]
async fn a_write_whose_publication_failed_is_not_replayed_as_committed() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("notes.txt", "tail\n");
    repo.open_remote_pr_on_branch().await;
    repo.break_origin();

    let payload = json!({
        "changes": [{
            "target": "file:notes.txt",
            "mode": "patch",
            "payload": { "old_string": "tail", "new_string": "head\ntail" }
        }],
        "commit_msg": "prepend head"
    });

    let first = repo
        .change_report_as_tool_use("toolu_unpublished", payload.clone())
        .await;
    assert_eq!(
        first["commit"]["status"], "sealed locally; unpublished",
        "a commit whose required push failed must not report as committed: {first:?}"
    );

    let replay = repo
        .change_report_as_tool_use("toolu_unpublished", payload)
        .await;
    assert_eq!(
        replay["commit"]["status"], "sealed locally; unpublished",
        "the redelivery must be answered with what actually happened: {replay:?}"
    );
    assert_eq!(
        replay["commit"]["sha"], first["commit"]["sha"],
        "and with the commit the first delivery landed, not a second one"
    );
    assert_eq!(
        repo.read("notes.txt"),
        "head\ntail\n",
        "the redelivery must still not re-apply the patch"
    );
}

/// Bind an open remote pull request to this fixture's own job, so a create-pr
/// artifact write on that job has a pull request to sync.
async fn bind_remote_pr_to_job(repo: &ChangeTestRepo) {
    repo.orch
        .db
        .local
        .execute(
            "INSERT INTO merge_requests
             (id, job_id, project_id, title, source_branch, target_branch, status, is_local, opened_at, updated_at, github_pr_number)
             VALUES ('mr-deferred', 'job-change', ?1, 'Old title', 'agent/CHG-1-builder-0', 'main', 'open', 0, 1, 1, 4242)",
            cairn_db::turso::params![repo.project_id.clone()],
        )
        .await
        .unwrap();
}

fn file_and_create_pr_payload() -> serde_json::Value {
    json!({
        "changes": [
            {
                "target": "file:notes.txt",
                "mode": "patch",
                "payload": { "old_string": "tail", "new_string": "head\ntail" }
            },
            {
                "target": "cairn://p/chg/1/1/builder/create-pr",
                "mode": "create",
                "payload": { "title": "Deferred title", "body": "Deferred body" }
            }
        ],
        "commit_msg": "prepend head"
    })
}

/// A batch that edits files holds its store lock and branch reservation while
/// it dispatches a create-pr artifact. The artifact's pull-request sync is
/// network I/O, so it runs in completion: when it edits the pull request,
/// neither lock is held. A sync that fails there is reported in the final
/// report, the replay ledger carries that report, and a redelivery is answered
/// with it without editing the pull request again.
#[tokio::test]
async fn a_mixed_batch_syncs_its_create_pr_artifact_after_releasing_its_locks() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("notes.txt", "tail\n");
    bind_remote_pr_to_job(&repo).await;
    let edits = cairn_core::pr_data::actions::prose_edit_seam::install(repo.project.path(), |_| {
        Err("scripted: GitHub refused the edit".to_string())
    });

    let first = repo
        .change_report_as_tool_use("toolu_deferred_sync", file_and_create_pr_payload())
        .await;

    assert!(first["commit"]["sha"].is_string(), "{first:?}");
    assert_eq!(repo.read("notes.txt"), "head\ntail\n");
    let probes = edits.probes();
    assert_eq!(probes.len(), 1, "one pull-request edit: {probes:?}");
    assert_eq!(probes[0].pr_number, 4242);
    assert_eq!(
        probes[0].store_lock_holder, None,
        "the pull request is edited with the store lock free"
    );
    assert_eq!(
        probes[0].branch_reservation_holder, None,
        "and with the branch reservation free"
    );
    let error = failure_error(&first, 1);
    assert!(
        error.contains("was not synced") && error.contains("scripted: GitHub refused the edit"),
        "{first:?}"
    );
    let summary = first["applied"]
        .as_array()
        .unwrap()
        .iter()
        .find(|change| change["index"] == 1)
        .map(|change| change["summary"].as_str().unwrap().to_string())
        .expect("the artifact was stored");
    assert!(summary.contains("pull request not synced"), "{summary}");
    assert!(!summary.contains("sync pending"), "{summary}");

    let replay = repo
        .change_report_as_tool_use("toolu_deferred_sync", file_and_create_pr_payload())
        .await;
    assert_eq!(replay["commit"]["sha"], first["commit"]["sha"]);
    assert!(
        failure_error(&replay, 1).contains("was not synced"),
        "the redelivery is answered with the failed sync: {replay:?}"
    );
    assert_eq!(edits.probes().len(), 1, "a redelivery edits nothing again");
}

/// When the batch's own branch push fails, its deferred create-pr syncs are
/// reported unsynced and nothing pushes or edits the pull request again.
#[tokio::test]
async fn a_failed_branch_push_leaves_the_deferred_create_pr_sync_unsynced() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("notes.txt", "tail\n");
    bind_remote_pr_to_job(&repo).await;
    repo.break_origin();
    let edits =
        cairn_core::pr_data::actions::prose_edit_seam::install(repo.project.path(), |_| Ok(()));

    let first = repo
        .change_report_as_tool_use("toolu_deferred_unpublished", file_and_create_pr_payload())
        .await;

    assert_eq!(
        first["commit"]["status"], "sealed locally; unpublished",
        "{first:?}"
    );
    assert!(
        failure_error(&first, 1).contains("branch push failed first"),
        "{first:?}"
    );
    assert!(
        edits.probes().is_empty(),
        "no pull-request edit after a failed push"
    );
    let replay = repo
        .change_report_as_tool_use("toolu_deferred_unpublished", file_and_create_pr_payload())
        .await;
    assert!(
        failure_error(&replay, 1).contains("branch push failed first"),
        "{replay:?}"
    );
}

/// The guard keys on the tool call, not on the payload: a genuine second edit
/// the agent meant to make carries its own `tool_use_id` and must still apply.
#[tokio::test]
async fn distinct_tool_use_ids_still_apply_independently() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write(
        "contracts.rs",
        "pub const PROJECT_IMAGE_CONTRACT: Contract = Contract {\n    name: \"image\",\n};\n",
    );

    let first = repo
        .change_report_as_tool_use("toolu_one", anchor_preserving_insertion("first insert"))
        .await;
    assert_successful_change(&first, 1);
    let second = repo
        .change_report_as_tool_use("toolu_two", anchor_preserving_insertion("second insert"))
        .await;
    assert_successful_change(&second, 1);

    let landed = repo.read("contracts.rs");
    assert_eq!(
        landed.matches("PROJECT_IMAGES_CONTRACT").count(),
        2,
        "distinct tool calls are distinct edits: {landed}"
    );
    assert_ne!(first["commit"]["sha"], second["commit"]["sha"]);
}

/// A delivery with no `tool_use_id` cannot be correlated to a prior one, so it
/// applies as it always has rather than being keyed under an invented identity.
#[tokio::test]
async fn a_write_without_a_tool_use_id_applies_as_today() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write(
        "contracts.rs",
        "pub const PROJECT_IMAGE_CONTRACT: Contract = Contract {\n    name: \"image\",\n};\n",
    );

    assert_successful_change(
        &repo
            .change_report(anchor_preserving_insertion("unkeyed first"))
            .await,
        1,
    );
    assert_successful_change(
        &repo
            .change_report(anchor_preserving_insertion("unkeyed second"))
            .await,
        1,
    );

    let landed = repo.read("contracts.rs");
    assert_eq!(
        landed.matches("PROJECT_IMAGES_CONTRACT").count(),
        2,
        "an unkeyed write is not deduplicated: {landed}"
    );
}

/// Resource-only writes claim replay identity before dispatch, so a transport
/// redelivery returns the settled first report without appending twice.
#[tokio::test]
async fn stop_after_real_file_write_settlement_replays_authoritative_provider_context() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    let tool_use_id = "tool-stop-file";
    let payload = json!({
        "changes": [{"target": "file:stopped.txt", "mode": "create",
                     "payload": {"content": "durable\n"}}],
        "commit_msg": "write before stop"
    });
    record_write_call(&repo.orch.db.local, tool_use_id, &payload).await;
    let gate = gate_settled_delivery("run-change", tool_use_id);
    let mut request = make_request(repo.cwd(), payload);
    request.run_id = Some("run-change".to_string());
    request.tool_use_id = Some(tool_use_id.to_string());

    let (delivered, ()) = tokio::join!(handle_write(&repo.orch, &request), async {
        gate.entered().await;
        lifecycle::stop_session(&repo.orch, "run-change").unwrap();
        gate.release();
    });

    let report: serde_json::Value = serde_json::from_str(&delivered).unwrap();
    let sha = report["commit"]["sha"].as_str().unwrap();
    assert_eq!(sha, repo.branch_head());
    assert_eq!(repo.read("stopped.txt"), "durable\n");
    let stopped = stopped_result(&repo.orch.db.local, tool_use_id).await;
    let resumed = resumed_model_context(&repo);
    assert!(stopped.contains(sha), "{stopped}");
    assert!(resumed.contains(sha), "{resumed}");
    assert!(resumed.contains("Do not retry"), "{resumed}");
    assert!(
        !resumed.to_ascii_lowercase().contains("not written"),
        "{resumed}"
    );
}

#[tokio::test]
async fn stop_after_real_resource_write_settlement_replays_exactly_once_provider_context() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    let tool_use_id = "tool-stop-resource";
    let payload = json!({
        "changes": [{"target": "cairn://p/chg/messages", "mode": "append",
                     "payload": {"content": "settled once"}}]
    });
    record_write_call(&repo.orch.db.local, tool_use_id, &payload).await;
    let gate = gate_settled_delivery("run-change", tool_use_id);
    let mut request = make_request(repo.cwd(), payload);
    request.run_id = Some("run-change".to_string());
    request.tool_use_id = Some(tool_use_id.to_string());

    let (delivered, ()) = tokio::join!(handle_write(&repo.orch, &request), async {
        gate.entered().await;
        lifecycle::stop_session(&repo.orch, "run-change").unwrap();
        gate.release();
    });

    let report = parse_report(&delivered);
    assert_successful_change(&report, 1);
    assert_eq!(
        common::scalar_i64_by_id(
            &repo.orch.db.local,
            "SELECT COUNT(*) FROM messages WHERE channel_type = 'project' AND channel_id = (SELECT key FROM projects WHERE id = ?1) AND content = 'settled once'",
            &repo.project_id,
        )
        .await,
        1
    );
    let stopped = stopped_result(&repo.orch.db.local, tool_use_id).await;
    let resumed = resumed_model_context(&repo);
    let stopped_event = parse_report(&stopped);
    let stopped_report = parse_report(stopped_event["toolResult"].as_str().unwrap());
    assert_eq!(stopped_report["applied"], report["applied"]);
    let summary = report["applied"][0]["summary"].as_str().unwrap();
    assert!(!summary.is_empty(), "{report}");
    assert!(resumed.contains(summary), "{resumed}");
    assert!(resumed.contains("Do not retry"), "{resumed}");
    assert!(
        !resumed.to_ascii_lowercase().contains("not written"),
        "{resumed}"
    );
    assert_eq!(
        common::scalar_i64_by_id(
            &repo.orch.db.local,
            "SELECT COUNT(*) FROM events WHERE run_id = ?1 AND event_type = 'tool_result'",
            "run-change",
        )
        .await,
        1
    );
}

/// Three message appends, `item-0` to `item-2`, as one guarded call.
fn three_appends() -> serde_json::Value {
    json!({
        "changes": (0..3).map(|n| json!({
            "target": "cairn://p/chg/messages",
            "mode": "append",
            "payload": {"content": format!("item-{n}")}
        })).collect::<Vec<_>>()
    })
}

async fn guarded_request(
    repo: &ChangeTestRepo,
    tool_use_id: &str,
    payload: &serde_json::Value,
) -> McpCallbackRequest {
    record_write_call(&repo.orch.db.local, tool_use_id, payload).await;
    let mut request = make_request(repo.cwd(), payload.clone());
    request.run_id = Some("run-change".to_string());
    request.tool_use_id = Some(tool_use_id.to_string());
    request
}

async fn item_counts(repo: &ChangeTestRepo) -> Vec<i64> {
    let mut counts = Vec::new();
    for n in 0..3 {
        counts.push(
            repo.orch
                .db
                .local
                .query_opt_i64(
                    "SELECT COUNT(*) FROM messages WHERE content = ?1",
                    params![format!("item-{n}")],
                )
                .await
                .unwrap()
                .unwrap_or(0),
        );
    }
    counts
}

fn indices(report: &serde_json::Value, key: &str) -> Vec<u64> {
    report[key]
        .as_array()
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| entry["index"].as_u64())
                .collect()
        })
        .unwrap_or_default()
}

fn failure_error(report: &serde_json::Value, index: u64) -> String {
    report["failures"]
        .as_array()
        .and_then(|failures| failures.iter().find(|failure| failure["index"] == index))
        .and_then(|failure| failure["error"].as_str())
        .unwrap_or_default()
        .to_string()
}

/// A redelivery of the same call, bounded: a sealed row replays at once.
async fn replay_within(repo: &ChangeTestRepo, request: &McpCallbackRequest) -> serde_json::Value {
    let replayed = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        handle_write(&repo.orch, request),
    )
    .await
    .expect("a stopped write's row is sealed, so a replay answers within 5s");
    parse_report(&replayed)
}

/// cairn/5844 V13a: an operator Stop between items 1 and 2 stops the write
/// there. The stop's result and a replay both list 0-1 applied and 2 not
/// applied, and nothing runs twice.
#[tokio::test]
async fn a_write_stopped_between_items_reports_exactly_what_it_applied() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    let tool_use_id = "tool-stop-between-items";
    let payload = three_appends();
    let request = guarded_request(&repo, tool_use_id, &payload).await;
    let gate = gate_resource_item("run-change", tool_use_id, 2, ItemPhase::BeforeDispatch);

    let (delivered, ()) = tokio::join!(handle_write(&repo.orch, &request), async {
        gate.entered().await;
        let orch = repo.orch.clone();
        let stopper =
            tokio::task::spawn_blocking(move || lifecycle::stop_session(&orch, "run-change"));
        while !repo.orch.write_stops.is_raised("run-change", tool_use_id) {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        gate.release();
        stopper.await.unwrap().unwrap();
    });

    let delivered = parse_report(&delivered);
    assert_eq!(indices(&delivered, "applied"), vec![0, 1], "{delivered}");
    assert_eq!(indices(&delivered, "failures"), vec![2], "{delivered}");
    assert!(
        failure_error(&delivered, 2).starts_with("not applied"),
        "{delivered}"
    );
    assert_eq!(item_counts(&repo).await, vec![1, 1, 0]);

    let stopped = parse_report(&stopped_result(&repo.orch.db.local, tool_use_id).await);
    let stopped = parse_report(stopped["toolResult"].as_str().unwrap());
    assert_eq!(indices(&stopped, "applied"), vec![0, 1], "{stopped}");
    assert!(
        failure_error(&stopped, 2).starts_with("not applied"),
        "{stopped}"
    );
    assert!(
        stopped["interrupted_during_write"]
            .as_str()
            .is_some_and(|note| note.contains("do not resend them")),
        "{stopped}"
    );
    assert!(
        stopped.get("interrupted_after_effect").is_none(),
        "{stopped}"
    );

    let replayed = replay_within(&repo, &request).await;
    assert!(replayed.get("replayed").is_some(), "{replayed}");
    assert_eq!(indices(&replayed, "applied"), vec![0, 1], "{replayed}");
    assert_eq!(indices(&replayed, "failures"), vec![2], "{replayed}");
    assert_eq!(
        item_counts(&repo).await,
        vec![1, 1, 0],
        "the replay ran nothing"
    );
}

/// cairn/5844 V13b: a write whose future is dropped between items (the
/// transport aborting it) seals its row, so a replay reports 0-1 applied and
/// 2 not applied.
#[tokio::test]
async fn a_write_dropped_between_items_seals_what_it_applied() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    let tool_use_id = "tool-drop-between-items";
    let payload = three_appends();
    let request = guarded_request(&repo, tool_use_id, &payload).await;
    let gate = gate_resource_item("run-change", tool_use_id, 2, ItemPhase::BeforeDispatch);
    {
        let mut delivery = std::pin::pin!(handle_write(&repo.orch, &request));
        tokio::select! {
            _ = &mut delivery => panic!("the write must park before item 2"),
            () = gate.entered() => {}
        }
    }

    let replayed = replay_within(&repo, &request).await;
    assert_eq!(indices(&replayed, "applied"), vec![0, 1], "{replayed}");
    assert!(
        failure_error(&replayed, 2).starts_with("not applied"),
        "{replayed}"
    );
    assert_eq!(item_counts(&repo).await, vec![1, 1, 0]);
}

/// cairn/5844 V13c: dropped after item 2's dispatcher returned but before it
/// was recorded. Item 2 took effect; the replay calls it not confirmed and
/// does not run it again.
#[tokio::test]
async fn a_write_dropped_during_an_item_reports_it_not_confirmed() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    let tool_use_id = "tool-drop-during-item";
    let payload = three_appends();
    let request = guarded_request(&repo, tool_use_id, &payload).await;
    let gate = gate_resource_item("run-change", tool_use_id, 2, ItemPhase::AfterDispatch);
    {
        let mut delivery = std::pin::pin!(handle_write(&repo.orch, &request));
        tokio::select! {
            _ = &mut delivery => panic!("the write must park after item 2 dispatched"),
            () = gate.entered() => {}
        }
    }
    assert_eq!(item_counts(&repo).await, vec![1, 1, 1]);

    let replayed = replay_within(&repo, &request).await;
    assert_eq!(indices(&replayed, "applied"), vec![0, 1], "{replayed}");
    assert!(
        failure_error(&replayed, 2).starts_with("not confirmed"),
        "{replayed}"
    );
    assert_eq!(
        item_counts(&repo).await,
        vec![1, 1, 1],
        "the replay ran nothing"
    );
}

/// cairn/5844 V13d: per-item checkpoints leave the row behind the pending
/// marker with a body, and the blocking group's own checkpoint still takes
/// it.
#[tokio::test]
async fn a_resource_append_then_a_blocking_append_still_checkpoints() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    let payload = json!({
        "changes": [
            {"target": "cairn://p/chg/messages", "mode": "append", "payload": {"content": "item-0"}},
            {"target": "cairn://p/chg/1/1/builder/tasks", "mode": "append",
             "payload": {"subagentType": "Explore", "description": "noop", "prompt": "noop"}}
        ]
    });
    let request = guarded_request(&repo, "tool-then-blocking", &payload).await;
    let delivered = handle_write(&repo.orch, &request).await;
    let report = parse_report(delivered.split("\n\n").next().unwrap());
    assert_eq!(indices(&report, "applied")[0], 0, "{report}");
    assert!(
        !failure_error(&report, 1).contains("Could not checkpoint"),
        "the blocking group's checkpoint must take a row with item progress: {report}"
    );
    assert_eq!(item_counts(&repo).await, vec![1, 0, 0]);
}

#[tokio::test]
async fn a_resource_only_write_is_deduplicated_by_the_replay_guard() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    let payload = json!({
        "changes": [{
            "target": "cairn://p/chg/messages",
            "mode": "append",
            "payload": { "content": "a message" }
        }]
    });

    let first = repo
        .change_report_as_tool_use("toolu_res", payload.clone())
        .await;
    let second = repo.change_report_as_tool_use("toolu_res", payload).await;

    assert_successful_change(&first, 1);
    assert!(second["replayed"].is_string(), "{second:?}");
    assert_eq!(second["applied"], first["applied"]);
    assert_eq!(
        count_rows(
            &repo.orch.db.local,
            "SELECT COUNT(*) FROM messages WHERE channel_type = 'project' AND content = 'a message'"
        )
        .await,
        1,
        "the redelivery must not append a second message"
    );
}

/// A settled failure is replayed as the same failure. It is not treated as an
/// empty claim that permits another pass through a partially effectful batch.
#[tokio::test]
async fn a_failed_resource_only_write_replays_its_failure() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    let payload = json!({
        "atomic": true,
        "changes": [{
            "target": "cairn://p/missing/messages",
            "mode": "append",
            "payload": { "content": "never appended" }
        }]
    });

    let first = repo
        .change_report_as_tool_use("toolu_resource_failure", payload.clone())
        .await;
    let replay = repo
        .change_report_as_tool_use("toolu_resource_failure", payload)
        .await;

    assert_eq!(failure_count(&first), 1, "{first:?}");
    assert!(replay["replayed"].is_string(), "{replay:?}");
    assert_eq!(replay["failures"], first["failures"]);
}

fn revert_payload(commit: &str) -> serde_json::Value {
    json!({
        "changes": [{
            "target": "file:",
            "mode": "revert",
            "payload": { "commit": commit }
        }],
        "commit_msg": "revert selected change"
    })
}

#[tokio::test]
#[serial_test::serial(jj)]
async fn handler_revert_preserves_disjoint_work_reports_paths_and_records_ledger() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };

    let selected = repo
        .change_report(json!({
            "changes": [
                { "target": "file:selected.txt", "mode": "create", "payload": { "content": "selected\n" } },
                { "target": "file:also-selected.txt", "mode": "create", "payload": { "content": "also\n" } }
            ],
            "commit_msg": "selected change"
        }))
        .await;
    assert_successful_change(&selected, 2);
    let selected_sha = selected["commit"]["sha"].as_str().unwrap();

    let later = repo
        .change_report(json!({
            "changes": [{
                "target": "file:later.txt",
                "mode": "create",
                "payload": { "content": "later\n" }
            }],
            "commit_msg": "disjoint later change"
        }))
        .await;
    assert_successful_change(&later, 1);

    let reverted = repo
        .change_report_as_tool_use("toolu_revert_success", revert_payload(selected_sha))
        .await;
    assert_successful_change(&reverted, 1);
    assert_eq!(reverted["applied"][0]["mode"], "revert");
    assert_eq!(reverted["applied"][0]["target"], "file:");
    assert_eq!(
        reverted["applied"][0]["summary"],
        format!("Reverted {selected_sha} (2 paths)")
    );
    assert!(!repo.logical_exists("selected.txt"));
    assert!(!repo.logical_exists("also-selected.txt"));
    assert_eq!(repo.read("later.txt"), "later\n");
    assert_eq!(
        count_rows(
            &repo.orch.db.local,
            "SELECT COUNT(*) FROM file_changes WHERE job_id = 'job-change' AND status = 'deleted' AND file_path IN ('selected.txt', 'also-selected.txt') AND additions = 0 AND deletions = 1"
        )
        .await,
        2,
        "the handler must ledger the inverse changes, not the original additions"
    );

    let replay = repo
        .change_report_as_tool_use("toolu_revert_success", revert_payload(selected_sha))
        .await;
    assert!(replay["replayed"].is_string(), "{replay:?}");
    assert_eq!(replay["commit"]["sha"], reverted["commit"]["sha"]);
    assert_eq!(replay["applied"], reverted["applied"]);
    assert_eq!(
        count_rows(
            &repo.orch.db.local,
            "SELECT COUNT(*) FROM file_changes WHERE job_id = 'job-change' AND status = 'deleted' AND file_path IN ('selected.txt', 'also-selected.txt')"
        )
        .await,
        2,
        "a replayed revert must not duplicate the file-change ledger"
    );
}

#[tokio::test]
#[serial_test::serial(jj)]
async fn handler_revert_overlap_failure_leaves_head_and_ledger_unchanged() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };

    let selected = repo
        .change_report(json!({
            "changes": [{
                "target": "file:shared.txt",
                "mode": "create",
                "payload": { "content": "selected\n" }
            }],
            "commit_msg": "selected change"
        }))
        .await;
    assert_successful_change(&selected, 1);
    let selected_sha = selected["commit"]["sha"].as_str().unwrap();
    let later = repo
        .change_report(json!({
            "changes": [{
                "target": "file:shared.txt",
                "mode": "replace",
                "payload": { "content": "later\n" }
            }],
            "commit_msg": "overlapping later change"
        }))
        .await;
    assert_successful_change(&later, 1);
    let ledger_before = count_rows(
        &repo.orch.db.local,
        "SELECT COUNT(*) FROM file_changes WHERE job_id = 'job-change'",
    )
    .await;

    let failed = repo
        .change_report_as_tool_use("toolu_revert_overlap", revert_payload(selected_sha))
        .await;
    assert_eq!(failure_count(&failed), 1, "{failed:?}");
    assert!(
        failed["failures"][0]["error"]
            .as_str()
            .unwrap()
            .contains("overlaps later edits at: shared.txt"),
        "{failed:?}"
    );
    assert_eq!(failed["applied"], json!([]), "{failed:?}");
    assert!(failed.get("partial_success").is_none(), "{failed:?}");
    assert!(failed.get("commit").is_none(), "{failed:?}");
    assert_eq!(repo.read("shared.txt"), "later\n");
    assert_eq!(
        count_rows(
            &repo.orch.db.local,
            "SELECT COUNT(*) FROM file_changes WHERE job_id = 'job-change'",
        )
        .await,
        ledger_before
    );

    let retry = repo
        .change_report_as_tool_use("toolu_revert_overlap", revert_payload(selected_sha))
        .await;
    assert_eq!(retry["failures"], failed["failures"]);
    assert!(
        retry.get("replayed").is_none(),
        "failed file writes remain retryable"
    );
    assert_eq!(retry["applied"], json!([]), "{retry:?}");
    assert_eq!(repo.read("shared.txt"), "later\n");
}

#[tokio::test]
async fn change_unified_patch_preview_hashes_expanded_targets() {
    let Some(repo) = ChangeTestRepo::try_new().await else {
        eprintln!("skipping: jj not resolvable");
        return;
    };
    repo.write("lib.rs", "old();\n");
    let payload = json!({
        "changes": [{
            "target": "file:",
            "mode": "unified_patch",
            "payload": { "patch": "*** Begin Patch\n*** Add File: add.rs\n+add();\n*** Update File: lib.rs\n@@ -1,1 +1,1 @@\n-old();\n+new();\n*** End Patch\n" }
        }],
        "commit_msg": "add files",
        "preview": true
    });
    insert_preview_event(&repo.orch.db.local, payload.clone()).await;

    let report = repo.preview_report(payload).await;

    assert_successful_change(&report, 2);
    assert_eq!(repo.read("lib.rs"), "old();\n");
    assert!(!repo.path("add.rs").exists());
    let targets = report["target_hashes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|hash| hash["target"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(targets, vec!["file:add.rs", "file:lib.rs"]);
}

async fn insert_preview_event(db: &LocalDb, input: serde_json::Value) {
    let project_id = common::create_project(db, "preview").await;
    db.write(|conn| {
        let project_id = project_id.clone();
        let data = json!({
            "toolUses": [{
                "id": "toolu-preview",
                "name": "mcp__cairn__change",
                "input": input,
            }]
        })
        .to_string();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO jobs(id, project_id, status, current_session_id, created_at, updated_at)
                 VALUES ('job-preview', ?1, 'running', 'session-preview', 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO sessions(id, job_id, status, created_at, updated_at)
                 VALUES ('session-preview', 'job-preview', 'active', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, updated_at, start_mode)
                 VALUES ('run-preview', ?1, 'job-preview', 'live', 'session-preview', 1, 1, 'resume')",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data, created_at)
                 VALUES ('event-preview', 'run-preview', 'session-preview', 1, 1, 'assistant', ?1, 1)",
                params![data.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

fn make_preview_request(cwd: &str, payload: serde_json::Value) -> McpCallbackRequest {
    McpCallbackRequest {
        answer_bound_ms: None,
        thread_id: None,
        cwd: cwd.to_string(),
        run_id: Some("run-preview".to_string()),
        tool: "write".to_string(),
        payload,
        tool_use_id: Some("toolu-preview".to_string()),
        origin: None,
    }
}
