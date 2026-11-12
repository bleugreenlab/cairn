//! One direct message to several addresses (cairn/5537): stored once, one
//! recipient row and one `direct:` push per recipient, each recipient's own
//! mute deciding its wake, every envelope naming the others, and a reply that
//! reaches only the address it names. Every recipient's head turn is running,
//! so a send takes the queue path and spawns no process.

use crate::common;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use cairn_core::internal::dispatch::dispatch_tool;
use cairn_core::internal::mcp::handlers::issue_resources::handle_read_issue_resource;
use cairn_core::internal::mcp::handlers::write::handle_write;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::storage::LocalDb;

const ALPHA: &str = "cairn://p/proj/42/1/alpha";
const BETA: &str = "cairn://p/proj/42/1/beta";
const GAMMA: &str = "cairn://p/proj/42/1/gamma";
const SENDER: &str = "cairn://p/proj/42/1/sender";

fn request(tool: &str, run_id: Option<&str>, payload: serde_json::Value) -> McpCallbackRequest {
    McpCallbackRequest {
        answer_bound_ms: None,
        thread_id: None,
        cwd: "/tmp".to_string(),
        run_id: run_id.map(str::to_string),
        tool: tool.to_string(),
        payload,
        tool_use_id: None,
        origin: None,
    }
}

/// A model-facing call under `run`, the kind that drains the run's pushes at
/// its tool boundary.
fn boundary(run: &str) -> McpCallbackRequest {
    let mut request = request("bogus_tool", Some(run), serde_json::json!({}));
    cairn_common::protocol::stamp_callback_origin(
        &mut request.payload,
        cairn_common::protocol::MODEL_FACING_ORIGIN,
    );
    request
}

/// `alpha`, `beta`, `gamma` and `sender` on issue proj-42, execution 1, each
/// with a running head turn; a `waiting` node that has not started; and node
/// `done` on issue 43, which is closed.
async fn seed(db: &LocalDb) {
    let mut script = String::from(
        "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
           VALUES ('proj-1', 'default', 'Test Project', 'proj', '/tmp/test-repo', 1, 1);
         INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
           VALUES ('issue-1', 'proj-1', 42, 'live', 'active', 1, 1),
                  ('issue-2', 'proj-1', 43, 'closed', 'closed', 1, 1);
         INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
           VALUES ('exec-1', 'recipe-default', 'issue-1', 'proj-1', 'running', 1, 1),
                  ('exec-2', 'recipe-default', 'issue-2', 'proj-1', 'complete', 1, 1);
         INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, node_name, uri_segment, status, created_at, updated_at)
           VALUES ('job-waiting', 'exec-1', 'waiting', 'issue-1', 'proj-1', 'waiting', 'waiting', 'pending', 1, 1);",
    );
    for (node, issue, exec) in [
        ("alpha", "issue-1", "exec-1"),
        ("beta", "issue-1", "exec-1"),
        ("gamma", "issue-1", "exec-1"),
        ("sender", "issue-1", "exec-1"),
        ("done", "issue-2", "exec-2"),
    ] {
        script.push_str(&format!(
            "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, node_name, uri_segment, status, current_session_id, created_at, updated_at)
               VALUES ('job-{node}', '{exec}', '{node}', '{issue}', 'proj-1', '{node}', '{node}', 'running', 'session-{node}', 1, 1);
             INSERT INTO sessions (id, job_id, status, created_at, updated_at)
               VALUES ('session-{node}', 'job-{node}', 'active', 1, 1);
             INSERT INTO runs (id, project_id, job_id, issue_id, status, session_id, created_at, updated_at, start_mode)
               VALUES ('run-{node}', 'proj-1', 'job-{node}', '{issue}', 'live', 'session-{node}', 1, 1, 'resume');
             INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, created_at, updated_at)
               VALUES ('turn-{node}', 'session-{node}', 'run-{node}', 'job-{node}', 1, 'running', 1, 1);
             UPDATE jobs SET current_turn_id = 'turn-{node}' WHERE id = 'job-{node}';"
        ));
    }
    db.execute_script(&script).await.unwrap();
}

async fn setup() -> (tempfile::TempDir, Orchestrator) {
    let (temp, orch) = common::test_orchestrator().await;
    seed(&orch.db.local).await;
    (temp, orch)
}

/// Append to `target` through the write path, from `sender_run` (`None` is
/// the operator), and return what the sender is told: the applied summary, or
/// the refusal.
async fn send(
    orch: &Orchestrator,
    sender_run: Option<&str>,
    target: &str,
    payload: serde_json::Value,
) -> String {
    let raw = handle_write(
        orch,
        &request(
            "write",
            sender_run,
            serde_json::json!({"changes": [{"target": target, "mode": "append", "payload": payload}]}),
        ),
    )
    .await;
    let report: serde_json::Value = serde_json::from_str(&raw).unwrap_or_default();
    let told = |key: &str, field: &str| {
        report[key]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|item| item[field].as_str())
            .collect::<Vec<_>>()
            .join("\n")
    };
    // The report itself follows, for whatever the two fields do not carry.
    [told("applied", "summary"), told("failures", "error"), raw].join("\n")
}

async fn count(db: &LocalDb, sql: &str) -> i64 {
    db.query_one(sql.to_string(), (), |row| {
        cairn_core::internal::storage::RowExt::i64(row, 0)
    })
    .await
    .unwrap()
}

async fn counts(db: &LocalDb) -> (i64, i64, i64) {
    (
        count(db, "SELECT COUNT(*) FROM messages").await,
        count(db, "SELECT COUNT(*) FROM message_recipients").await,
        count(db, "SELECT COUNT(*) FROM attention_pushes").await,
    )
}

async fn only_message_id(db: &LocalDb) -> String {
    db.query_one(
        "SELECT id FROM messages WHERE channel_type = 'direct'".to_string(),
        (),
        |row| cairn_core::internal::storage::RowExt::text(row, 0),
    )
    .await
    .unwrap()
}

/// `(recipient job, wake)` of each `direct:` push.
async fn direct_pushes(db: &LocalDb) -> Vec<(String, String)> {
    db.query_all(
        "SELECT recipient, wake FROM attention_pushes WHERE key LIKE 'direct:%' ORDER BY recipient"
            .to_string(),
        (),
        |row| {
            Ok((
                cairn_core::internal::storage::RowExt::text(row, 0)?,
                cairn_core::internal::storage::RowExt::text(row, 1)?,
            ))
        },
    )
    .await
    .unwrap()
}

async fn mute_sender_for(orch: &Orchestrator, job: &str) {
    let db = &orch.db.local;
    cairn_core::internal::orchestrator::wakes::seed_default_job_subscriptions(db, job)
        .await
        .unwrap();
    cairn_core::internal::orchestrator::wakes::subscribe(
        db,
        job,
        "peer",
        Some(SENDER),
        None,
        "system",
    )
    .await
    .unwrap();
    cairn_core::internal::orchestrator::wakes::mute(
        db,
        job,
        "peer",
        Some(SENDER),
        None,
        None,
        None,
        "system",
    )
    .await
    .unwrap();
}

async fn drained(orch: &Orchestrator, run: &str) -> String {
    dispatch_tool(orch, &boundary(run), &Mutex::new(HashMap::new()))
        .await
        .reminders
        .join("\n")
}

/// One append to alpha with beta and gamma in `cc` stores one message, three
/// recipient rows in the order named and three pushes, and each recipient's
/// envelope names exactly the other two and cites the message.
#[tokio::test(flavor = "current_thread")]
async fn one_append_to_three_addresses_stores_one_message_and_wakes_each_once() {
    let (_temp, orch) = setup().await;
    let db = orch.db.local.clone();

    let reply = send(
        &orch,
        Some("run-sender"),
        &format!("{ALPHA}/messages"),
        serde_json::json!({"content": "slot 3 is free", "cc": [BETA, format!("{GAMMA}/messages")]}),
    )
    .await;

    let id = only_message_id(&db).await;
    let uri = format!("cairn://p/proj/messages/{id}");
    assert!(
        reply.contains(&format!(
            "Sent direct message {uri} to {ALPHA}, {BETA}, {GAMMA}."
        )),
        "{reply}"
    );
    assert_eq!(counts(&db).await.0, 1, "one stored message");
    let rows = db
        .query_all(
            "SELECT position, recipient_job_id, recipient_run_id, wake FROM message_recipients ORDER BY position"
                .to_string(),
            (),
            |row| {
                use cairn_core::internal::storage::RowExt;
                Ok((row.i64(0)?, row.text(1)?, row.text(2)?, row.text(3)?))
            },
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        [
            (0, "job-alpha".into(), "run-alpha".into(), "wake".into()),
            (1, "job-beta".into(), "run-beta".into(), "wake".into()),
            (2, "job-gamma".into(), "run-gamma".into(), "wake".into()),
        ]
    );
    assert_eq!(
        direct_pushes(&db).await,
        [
            ("job-alpha".into(), "wake".into()),
            ("job-beta".into(), "wake".into()),
            ("job-gamma".into(), "wake".into()),
        ]
    );

    for (run, me, others) in [
        ("run-alpha", ALPHA, [BETA, GAMMA]),
        ("run-beta", BETA, [ALPHA, GAMMA]),
        ("run-gamma", GAMMA, [ALPHA, BETA]),
    ] {
        let envelope = drained(&orch, run).await;
        assert!(
            envelope.contains(&format!(
                "[Direct message from {SENDER}, also sent to {}, {}] slot 3 is free",
                others[0], others[1]
            )),
            "{run}: {envelope}"
        );
        assert!(
            !envelope.contains(&format!("also sent to {me}")),
            "{run} is not told it was sent to itself: {envelope}"
        );
        assert!(envelope.contains(&format!("Cite as {uri}.")), "{envelope}");
        assert!(
            envelope.contains("A reply reaches only the address it names"),
            "{envelope}"
        );
    }
}

/// A recipient that muted the sender is not woken and records why, while the
/// others are; `escalate:true` is `interrupt`, which wakes through a mute.
#[tokio::test(flavor = "current_thread")]
async fn each_recipient_s_own_mute_decides_its_wake() {
    let (_temp, orch) = setup().await;
    let db = orch.db.local.clone();
    mute_sender_for(&orch, "job-beta").await;

    let reply = send(
        &orch,
        Some("run-sender"),
        &format!("{ALPHA}/messages"),
        serde_json::json!({"content": "check in", "cc": [BETA, GAMMA]}),
    )
    .await;
    assert!(
        reply.contains(&format!("- {BETA}: The recipient muted you")),
        "{reply}"
    );
    for unmuted in [ALPHA, GAMMA] {
        assert!(
            !reply.contains(&format!("- {unmuted}: The recipient muted you")),
            "only the recipient that muted is told so: {reply}"
        );
    }
    assert_eq!(
        direct_pushes(&db).await,
        [
            ("job-alpha".into(), "wake".into()),
            ("job-beta".into(), "passive".into()),
            ("job-gamma".into(), "wake".into()),
        ]
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM message_recipients WHERE recipient_job_id = 'job-beta' AND wake = 'passive'"
        )
        .await,
        1
    );

    db.execute_script(
        "DELETE FROM attention_pushes; DELETE FROM message_recipients; DELETE FROM messages;",
    )
    .await
    .unwrap();
    send(
        &orch,
        Some("run-sender"),
        &format!("{ALPHA}/messages"),
        serde_json::json!({"content": "stop now", "cc": [BETA, GAMMA], "escalate": true}),
    )
    .await;
    assert_eq!(
        direct_pushes(&db).await,
        [
            ("job-alpha".into(), "interrupt".into()),
            ("job-beta".into(), "interrupt".into()),
            ("job-gamma".into(), "interrupt".into()),
        ],
        "escalation wakes through the mute, as `escalate` has always meant"
    );
}

/// A sender who asks for `passive` wakes no one and is told so, not that every
/// recipient muted it.
#[tokio::test(flavor = "current_thread")]
async fn a_passive_send_is_reported_as_passive_not_as_a_mute() {
    let (_temp, orch) = setup().await;
    let db = orch.db.local.clone();

    let single = send(
        &orch,
        Some("run-sender"),
        &format!("{ALPHA}/messages"),
        serde_json::json!({"content": "fyi", "urgency": "passive"}),
    )
    .await;
    assert!(!single.contains("muted you"), "{single}");
    assert!(single.contains("sent passively"), "{single}");

    let many = send(
        &orch,
        Some("run-sender"),
        &format!("{ALPHA}/messages"),
        serde_json::json!({"content": "fyi all", "urgency": "passive", "cc": [BETA, GAMMA]}),
    )
    .await;
    assert!(!many.contains("muted you"), "{many}");
    for home in [ALPHA, BETA, GAMMA] {
        assert!(
            many.contains(&format!("- {home}: It was sent passively")),
            "{home}: {many}"
        );
    }
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM attention_pushes WHERE key LIKE 'direct:%' AND wake <> 'passive'"
        )
        .await,
        0
    );
}

/// A recipient's reply names one address, the sender's, and wakes only it.
#[tokio::test(flavor = "current_thread")]
async fn a_reply_wakes_only_the_sender() {
    let (_temp, orch) = setup().await;
    let db = orch.db.local.clone();
    send(
        &orch,
        Some("run-sender"),
        &format!("{ALPHA}/messages"),
        serde_json::json!({"content": "who has the slot?", "cc": [BETA, GAMMA]}),
    )
    .await;
    db.execute_script("DELETE FROM attention_pushes;")
        .await
        .unwrap();

    send(
        &orch,
        Some("run-beta"),
        &format!("{SENDER}/messages"),
        serde_json::json!({"content": "I do"}),
    )
    .await;

    let woken: Vec<String> = direct_pushes(&db)
        .await
        .into_iter()
        .map(|(job, _)| job)
        .collect();
    assert_eq!(woken, ["job-sender"]);
}

/// Every bad address refuses the whole append, naming each one, and nothing
/// is stored.
#[tokio::test(flavor = "current_thread")]
async fn an_unresolvable_cc_refuses_the_append_with_nothing_stored() {
    let (_temp, orch) = setup().await;
    let db = orch.db.local.clone();
    let before = counts(&db).await;

    let reply = send(
        &orch,
        Some("run-sender"),
        &format!("{ALPHA}/messages"),
        serde_json::json!({
            "content": "hello",
            "cc": [
                BETA,
                "cairn://p/proj/42/1/nobody",
                "cairn://p/proj/42/1/waiting",
                "cairn://p/proj/43/1/done",
                format!("{ALPHA}/messages"),
                SENDER,
                "cairn://p/proj/issues",
            ],
        }),
    )
    .await;

    assert!(
        reply.contains("6 of the 8 addresses cannot receive this message"),
        "{reply}"
    );
    for (entry, reason) in [
        ("cairn://p/proj/42/1/nobody", "not found"),
        (
            "cairn://p/proj/42/1/waiting",
            "cairn://p/proj/42/1/waiting:",
        ),
        ("cairn://p/proj/43/1/done", "is terminal"),
        (
            &format!("{ALPHA}/messages") as &str,
            "names the same recipient as",
        ),
        (SENDER, "is you, the sender"),
        (
            "cairn://p/proj/issues",
            "is not an address a direct message can reach",
        ),
    ] {
        let line = reply
            .lines()
            .find(|line| line.starts_with(&format!("- {entry}: ")))
            .unwrap_or_else(|| panic!("{entry} is named: {reply}"));
        assert!(line.contains(reason), "{line}");
    }
    assert!(reply.contains("Nothing ran."), "{reply}");
    assert!(!reply.contains(&format!("- {BETA}:")), "{reply}");
    assert_eq!(counts(&db).await, before, "nothing was stored");
}

/// One message row lives in one database, so a cc in another (a team
/// project's replica) is refused, saying why and to send it separately.
#[tokio::test(flavor = "current_thread")]
async fn a_cc_in_another_database_is_refused_and_sent_separately() {
    let (_temp, orch) = setup().await;
    let (_team_temp, team) = common::migrated_db().await;
    let team = Arc::new(team);
    team.execute_script(
        "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
           VALUES ('tp', 'default', 'Team', 'tp', '/tmp/team', 1, 1);
         INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
           VALUES ('t-issue', 'tp', 1, 'team', 'active', 1, 1);
         INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
           VALUES ('t-exec', 'recipe-default', 't-issue', 'tp', 'running', 1, 1);
         INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, node_name, uri_segment, status, created_at, updated_at)
           VALUES ('t-job', 't-exec', 'builder', 't-issue', 'tp', 'builder', 'builder', 'running', 1, 1);
         INSERT INTO runs (id, project_id, job_id, issue_id, status, created_at, updated_at, start_mode)
           VALUES ('t-run', 'tp', 't-job', 't-issue', 'live', 1, 1, 'resume');",
    )
    .await
    .unwrap();
    orch.db.insert_team_db_for_test("team", team.clone()).await;
    orch.db.set_route("tp", Some("team".to_string())).await;
    let before = (counts(&orch.db.local).await, counts(&team).await);

    let reply = send(
        &orch,
        Some("run-sender"),
        &format!("{ALPHA}/messages"),
        serde_json::json!({"content": "hello", "cc": ["cairn://p/tp/1/1/builder"]}),
    )
    .await;

    assert!(
        reply.contains("- cairn://p/tp/1/1/builder: lives in a different database"),
        "{reply}"
    );
    assert!(
        reply.contains("send it to cairn://p/tp/1/1/builder separately"),
        "{reply}"
    );
    assert!(reply.contains("Nothing ran."), "{reply}");
    assert_eq!((counts(&orch.db.local).await, counts(&team).await), before);
}

/// Each recipient's `/messages` shows the message once with all three
/// recipients and its citable address, and so does the sender's; the address
/// reads as the message with its recipient list.
#[tokio::test(flavor = "current_thread")]
async fn every_recipient_s_history_and_the_message_uri_show_the_recipient_list() {
    let (_temp, orch) = setup().await;
    mute_sender_for(&orch, "job-gamma").await;
    send(
        &orch,
        Some("run-sender"),
        &format!("{ALPHA}/messages"),
        serde_json::json!({"content": "release slot 3", "cc": [BETA, GAMMA]}),
    )
    .await;
    let id = only_message_id(&orch.db.local).await;
    let uri = format!("cairn://p/proj/messages/{id}");

    let read = |uri: String| {
        let orch = &orch;
        async move {
            handle_read_issue_resource(
                orch,
                &request("read", None, serde_json::json!({ "uri": uri })),
            )
            .await
        }
    };
    for home in [ALPHA, BETA, GAMMA, SENDER] {
        let history = read(format!("{home}/messages")).await;
        assert_eq!(
            history.matches("release slot 3").count(),
            1,
            "{home}: {history}"
        );
        assert!(
            history.contains(&format!(
                " · {uri}] {SENDER} (steer) to {ALPHA}, {BETA}, {GAMMA}: release slot 3"
            )),
            "{home}: {history}"
        );
    }

    let message = read(uri.clone()).await;
    assert!(message.contains(&format!("# Message — {uri}")), "{message}");
    assert!(message.contains(&format!("- From: {SENDER}")), "{message}");
    assert!(
        message.contains(&format!(
            "- To:\n  1. {ALPHA} (wake)\n  2. {BETA} (wake)\n  3. {GAMMA} (passive)\n"
        )),
        "{message}"
    );
    assert!(message.contains("release slot 3"), "{message}");

    let elsewhere = read("cairn://p/proj/messages/no-such-message".to_string()).await;
    assert!(elsewhere.contains("not found"), "{elsewhere}");
}
