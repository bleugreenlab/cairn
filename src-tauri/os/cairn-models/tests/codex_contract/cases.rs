//! The contract cases. Each drives Cairn's own Codex client code (the real
//! `start_server`, `open`, `CodexControl`, `reconcile_goal`, one-shot
//! assembly, and wire parsers) against the real pinned binary, and returns a
//! [`CaseFailure`] naming itself. Where Cairn branches on Codex's human text,
//! the case also asserts the `errors.rs` predicate against the real message.

use std::time::{Duration, Instant};

use cairn_models::interface::session::CodexNativeTools;
use cairn_models::session::codex::managed::parse_codex_version;
use cairn_models::session::codex::params::{
    build_thread_fork_params, build_thread_resume_params, build_thread_start_params,
};
use cairn_models::session::codex::pin;
use cairn_models::session::codex::thread::reconcile_goal;
use cairn_models::session::events::codex::EventMapper;
use cairn_models::session::events::{SessionEvent, TurnStatus};
use cairn_models::session::one_shot::await_one_shot;
use cairn_models::wire::codex_app_server::errors::{
    active_turn_id_from_mismatch, is_missing_rollout_error, is_no_active_turn_error,
    is_selected_model_capacity_error,
};
use cairn_models::wire::codex_app_server::goal::{GoalStatus, GoalUpdatedNotification};
use cairn_models::wire::codex_app_server::{
    extract_mcp_result, notification_turn_id, Notification, ProviderTurnStatus,
    SUPPORTED_SERVER_REQUESTS,
};
use cairn_models::wire::MessageContent;
use serde_json::{json, Value};

use crate::env::{
    expired_chatgpt_login, fixture_mcp, method_of, thread_of, CaseResult, Check, ContractEnv, Open,
    Session, Stream, FAKE_MODEL,
};
use crate::fake_responses::{Mode, FAKE_TEXT};
use crate::surface::{sent_requests, Fixture, KnownDead, Schema};

const TURN: Duration = Duration::from_secs(60);

/// How long `thread/start` may take while a server request it triggered goes
/// unanswered. 0.163.0 answers in about 1.3 s (cairn/5138); Cairn's own
/// request timeout is 60 s.
const THREAD_START_BOUND: Duration = Duration::from_secs(15);

fn turn_completed<'a>(turn: &'a str) -> impl Fn(&Value) -> bool + 'a {
    move |message| {
        method_of(message) == Some("turn/completed") && notification_turn_id(message) == Some(turn)
    }
}

fn completed_status(message: &Value) -> Option<ProviderTurnStatus<'_>> {
    match Notification::parse(message) {
        Notification::TurnCompleted { status, .. } => Some(status),
        _ => None,
    }
}

/// Start a turn and read it to its end, keeping the thread's messages.
fn run_turn(
    c: Check,
    stream: &mut Stream,
    session: &mut Session,
    text: &str,
) -> Result<(String, Vec<Value>), crate::env::CaseFailure> {
    let turn = c.ok(
        session.turn(text),
        "starting a turn with Cairn's turn/start",
    )?;
    let thread = session.thread_id.clone();
    let mut messages = Vec::new();
    let done = stream.until(TURN, |message| {
        if thread_of(message) == Some(thread.as_str()) {
            messages.push(message.clone());
        }
        turn_completed(&turn)(message)
    });
    c.some(done, "turn/completed within 60 s")?;
    Ok((turn, messages))
}

pub fn managed_install_of_pinned_package(env: &ContractEnv) -> CaseResult {
    let c = Check("managed_install_of_pinned_package");
    let pin = pin::embedded_pin();
    let managed = c.setup(env.fresh_managed(), "staging a fresh install root")?;
    let resolved = c.ok(
        managed.ensure(),
        "installing the staged pinned package through ManagedCodex::ensure",
    )?;
    c.that(resolved.version == pin.version, || {
        format!(
            "installed {} but the pin is {}",
            resolved.version, pin.version
        )
    })?;
    let output = c.ok(
        std::process::Command::new(&resolved.path)
            .arg("--version")
            .output(),
        "running the installed codex --version",
    )?;
    let printed = String::from_utf8_lossy(&output.stdout).to_string();
    c.that(
        parse_codex_version(&printed) == Some(pin.version.as_str()),
        || {
            format!(
                "`codex --version` printed {printed:?}, not codex-cli {}",
                pin.version
            )
        },
    )
}

pub fn handshake_and_thread_start(env: &ContractEnv) -> CaseResult {
    let c = Check("handshake_and_thread_start");
    let mcp = fixture_mcp();
    let client = c.setup(
        env.launch(Some(&mcp), None),
        "starting the app-server with Cairn's initialize",
    )?;
    let session = c
        .ok(
            env.open(&client, Open::new().mcp(mcp)),
            "opening a thread with Cairn's thread/start params and MCP attachment check",
        )
        .map_err(|failure| {
            c.fail(format!(
                "{}\ncodex stderr:\n{}",
                failure.detail,
                env.stderr_tail()
            ))
        })?;
    c.that(!session.thread_id.is_empty(), || {
        "thread/start answered an empty thread id".to_string()
    })?;
    env.no_egress(c)
}

pub fn normal_turn_streams_and_completes(env: &ContractEnv) -> CaseResult {
    let c = Check("normal_turn_streams_and_completes");
    let client = c.setup(env.launch(None, None), "starting the app-server")?;

    // Cairn's one-shot assembly over the real stream.
    let mut one_shot = c.ok(env.open(&client, Open::new()), "opening a tool-free thread")?;
    c.ok(
        one_shot
            .launched
            .control
            .open_turn(&MessageContent::text("say hi")),
        "starting the one-shot turn",
    )?;
    let reply = c.ok(
        await_one_shot(one_shot.launched, TURN),
        "awaiting the one-shot reply",
    )?;
    c.that(reply.text == FAKE_TEXT, || {
        format!(
            "one-shot assembled {:?}, the provider streamed {FAKE_TEXT:?}",
            reply.text
        )
    })?;
    c.that(reply.usage.is_some(), || {
        "the one-shot reply carried no usage".to_string()
    })?;

    // The same turn read message by message through Cairn's parsers.
    let mut session = c.ok(
        env.open(&client, Open::new()),
        "opening a second tool-free thread",
    )?;
    let mut stream = env.stream(&client);
    let (turn, messages) = run_turn(c, &mut stream, &mut session, "say hi")?;
    let mut mapper = EventMapper::new();
    let mut events = Vec::new();
    let (mut started, mut deltas, mut usage) = (false, 0, None);
    for message in &messages {
        events.extend(mapper.map(message));
        match Notification::parse(message) {
            Notification::TurnStarted { turn_id } => started |= turn_id == Some(turn.as_str()),
            Notification::AgentMessageDelta { delta: Some(_) } => deltas += 1,
            Notification::TokenUsageUpdated {
                params: Some(params),
            } => usage = Some(params.clone()),
            _ => {}
        }
    }
    c.that(started, || {
        "turn/started never carried the turn id at params.turn.id".to_string()
    })?;
    c.that(deltas > 0, || {
        "no item/agentMessage/delta parsed as a text delta".to_string()
    })?;
    let usage = c.some(usage, "thread/tokenUsage/updated")?;
    for pointer in [
        "/tokenUsage/total/totalTokens",
        "/tokenUsage/last/totalTokens",
        "/tokenUsage/modelContextWindow",
    ] {
        c.that(
            usage.pointer(pointer).and_then(Value::as_u64).is_some(),
            || format!("thread/tokenUsage/updated has no {pointer}: {usage}"),
        )?;
    }
    let done = messages.last().cloned().unwrap_or_default();
    c.that(Notification::parse(&done).ends_turn(), || {
        "turn/completed does not end the turn".to_string()
    })?;
    c.that(
        completed_status(&done) == Some(ProviderTurnStatus::Completed),
        || format!("the turn ended {done}"),
    )?;
    let complete = events.iter().find_map(|event| match event {
        SessionEvent::TurnComplete {
            status,
            text,
            usage,
            ..
        } => Some((status, text, usage)),
        _ => None,
    });
    let (status, text, usage) = c.some(complete, "a TurnComplete session event")?;
    c.that(
        matches!(status, TurnStatus::Completed)
            && text.as_deref() == Some(FAKE_TEXT)
            && usage.is_some(),
        || format!("EventMapper ended the turn {status:?} with text {text:?} and usage {usage:?}"),
    )?;
    env.no_egress(c)
}

/// A slow turn that has started streaming, and the thread it runs on.
fn slow_turn(
    c: Check,
    env: &ContractEnv,
) -> Result<
    (
        std::sync::Arc<cairn_models::codex::app_server::AppServerClient>,
        Session,
        Stream,
        String,
    ),
    crate::env::CaseFailure,
> {
    let client = c.setup(env.launch(None, None), "starting the app-server")?;
    let mut session = c.ok(env.open(&client, Open::new()), "opening a tool-free thread")?;
    let mut stream = env.stream(&client);
    env.fake.queue([Mode::Slow]);
    let turn = c.ok(session.turn("count slowly"), "starting the slow turn")?;
    let thread = session.thread_id.clone();
    c.some(
        stream.until(TURN, |message| {
            thread_of(message) == Some(thread.as_str())
                && method_of(message) == Some("item/agentMessage/delta")
        }),
        "the slow turn streaming",
    )?;
    Ok((client, session, stream, turn))
}

pub fn turn_interrupt_exact(env: &ContractEnv) -> CaseResult {
    let c = Check("turn_interrupt_exact");
    let (_client, mut session, mut stream, turn) = slow_turn(c, env)?;
    c.ok(
        session.control().interrupt(),
        "CodexControl::interrupt on the live turn",
    )?;
    let done = c.some(
        stream.until(Duration::from_secs(20), turn_completed(&turn)),
        "turn/completed after the interrupt",
    )?;
    c.that(
        completed_status(&done) == Some(ProviderTurnStatus::Interrupted),
        || format!("the interrupted turn ended {done}"),
    )?;
    env.no_egress(c)
}

pub fn interrupt_after_completion_is_success(env: &ContractEnv) -> CaseResult {
    let c = Check("interrupt_after_completion_is_success");
    let client = c.setup(env.launch(None, None), "starting the app-server")?;
    let mut session = c.ok(env.open(&client, Open::new()), "opening a tool-free thread")?;
    let mut stream = env.stream(&client);
    let (turn, _) = run_turn(c, &mut stream, &mut session, "say hi")?;
    let raw = client.send_request(
        "turn/interrupt",
        json!({"threadId": session.thread_id, "turnId": turn}),
    );
    let error = match raw {
        Ok(result) => {
            return Err(c.fail(format!(
                "Codex accepted turn/interrupt for a finished turn ({result}); Cairn recognizes only its error text as already-stopped"
            )))
        }
        Err(error) => error,
    };
    c.that(is_no_active_turn_error(&error), || {
        format!("Codex's text for interrupting a finished turn is {error}, which is_no_active_turn_error does not match")
    })?;
    c.ok(
        session.control().interrupt(),
        "CodexControl::interrupt after the turn completed",
    )?;
    env.no_egress(c)
}

pub fn interrupt_mismatch_retries_found_id(env: &ContractEnv) -> CaseResult {
    let c = Check("interrupt_mismatch_retries_found_id");
    let (client, mut session, mut stream, turn) = slow_turn(c, env)?;
    let stale = "019a0000-0000-7000-8000-00000000c0de";
    let raw = client.send_request(
        "turn/interrupt",
        json!({"threadId": session.thread_id, "turnId": stale}),
    );
    let error = match raw {
        Ok(result) => {
            return Err(c.fail(format!(
                "Codex accepted turn/interrupt naming a stale turn ({result}); Cairn's retry needs the mismatch error"
            )))
        }
        Err(error) => error,
    };
    c.that(active_turn_id_from_mismatch(&error).as_deref() == Some(turn.as_str()), || {
        format!("Codex's mismatch text is {error}; active_turn_id_from_mismatch reads {:?}, the live turn is {turn}",
            active_turn_id_from_mismatch(&error))
    })?;
    *session.current_turn.lock().unwrap() = Some(stale.to_string());
    c.ok(
        session.control().interrupt(),
        "CodexControl::interrupt with a stale cached turn id",
    )?;
    let done = c.some(
        stream.until(Duration::from_secs(20), turn_completed(&turn)),
        "turn/completed after the retried interrupt",
    )?;
    c.that(
        completed_status(&done) == Some(ProviderTurnStatus::Interrupted),
        || format!("the retried interrupt left the turn ending {done}"),
    )?;
    env.no_egress(c)
}

/// Read until the thread's goal reaches `status`, through Cairn's own type.
fn goal_reaches(c: Check, stream: &mut Stream, thread: &str, status: GoalStatus) -> CaseResult {
    let wanted = serde_json::to_value(&status).expect("a goal status serializes");
    let update = c.some(
        stream.until(TURN, |message| {
            method_of(message) == Some("thread/goal/updated")
                && thread_of(message) == Some(thread)
                && message.pointer("/params/goal/status") == Some(&wanted)
        }),
        &format!("thread/goal/updated reaching {wanted}"),
    )?;
    let parsed: GoalUpdatedNotification = c.ok(
        serde_json::from_value(update["params"].clone()),
        "reading thread/goal/updated as Cairn's GoalUpdatedNotification",
    )?;
    c.that(parsed.goal.status == status, || {
        format!("the goal update read as {:?}", parsed.goal.status)
    })
}

pub fn goal_set_pause_reactivate_clear(env: &ContractEnv) -> CaseResult {
    let c = Check("goal_set_pause_reactivate_clear");
    let mcp = fixture_mcp();
    let client = c.setup(env.launch(Some(&mcp), None), "starting the app-server")?;
    let mut session = c.ok(env.open(&client, Open::new().mcp(mcp)), "opening a thread")?;
    let mut stream = env.stream(&client);
    let thread = session.thread_id.clone();
    let complete_once = || {
        [
            Mode::Tool {
                needle: "update_goal",
                arguments: r#"{"status":"complete"}"#,
            },
            Mode::Normal,
        ]
    };

    let objective = "Say hello once, then mark the goal complete";
    let paused = c.ok(
        session
            .control()
            .set_goal(Some(objective), Some(GoalStatus::Paused), None),
        "setting a paused goal (Cairn's durable-wait shape)",
    )?;
    c.that(
        paused.status == GoalStatus::Paused && paused.objective == objective,
        || format!("the paused set echoed {paused:?}"),
    )?;

    env.fake.queue(complete_once());
    let active = c.ok(
        session
            .control()
            .set_goal(None, Some(GoalStatus::Active), None),
        "reactivating the goal",
    )?;
    c.that(active.status == GoalStatus::Active, || {
        format!("reactivation echoed {active:?}")
    })?;
    goal_reaches(c, &mut stream, &thread, GoalStatus::Complete)?;

    c.ok(
        reconcile_goal(&client, &thread, None, true),
        "reconcile_goal clearing the goal",
    )?;
    let cleared = c.ok(
        session.control().get_goal(),
        "reading the goal after the clear",
    )?;
    c.that(cleared.is_none(), || {
        format!("the cleared thread still has {cleared:?}")
    })?;

    env.fake.queue(complete_once());
    c.ok(
        reconcile_goal(&client, &thread, Some("Say hello again"), false),
        "reconcile_goal setting an objective on a goalless thread",
    )?;
    goal_reaches(c, &mut stream, &thread, GoalStatus::Complete)?;
    c.ok(
        reconcile_goal(&client, &thread, None, true),
        "reconcile_goal clearing the second goal",
    )?;
    env.no_egress(c)
}

pub fn objectiveless_goal_set_on_goalless_thread(env: &ContractEnv) -> CaseResult {
    let c = Check("objectiveless_goal_set_on_goalless_thread");
    let mcp = fixture_mcp();
    let client = c.setup(env.launch(Some(&mcp), None), "starting the app-server")?;
    let mut session = c.ok(env.open(&client, Open::new().mcp(mcp)), "opening a thread")?;
    // cairn-core's goal pause treats exactly this pair as a no-op: the set
    // fails and the thread still has no goal.
    let set = session
        .control()
        .set_goal(None, Some(GoalStatus::Paused), None);
    c.that(set.is_err(), || {
        format!("an objective-less set on a goalless thread answered {set:?}")
    })?;
    let goal = c.ok(
        session.control().get_goal(),
        "reading the goal after the refused set",
    )?;
    c.that(goal.is_none(), || format!("the refused set left {goal:?}"))?;
    env.no_egress(c)
}

pub fn resume_unknown_thread_falls_back(env: &ContractEnv) -> CaseResult {
    let c = Check("resume_unknown_thread_falls_back");
    let mcp = fixture_mcp();
    let client = c.setup(env.launch(Some(&mcp), None), "starting the app-server")?;
    let unknown = uuid::Uuid::new_v4().to_string();
    let raw = client.send_request(
        "thread/resume",
        build_thread_resume_params(
            &unknown,
            &env.dir().join("work").to_string_lossy(),
            "never",
            "danger-full-access",
            Some(FAKE_MODEL),
            None,
            None,
            "contract",
            None,
        ),
    );
    let error = match raw {
        Ok(result) => return Err(c.fail(format!("Codex resumed a thread it never had ({result})"))),
        Err(error) => error,
    };
    c.that(is_missing_rollout_error(&error, &unknown), || {
        format!("Codex's text for resuming an unknown thread is {error}, which is_missing_rollout_error does not match")
    })?;
    let session = c.ok(
        env.open(&client, Open::new().mcp(mcp).resume(&unknown)),
        "opening a resume of the unknown thread",
    )?;
    c.that(session.thread_id != unknown, || {
        "the resume did not fall back to a new thread".to_string()
    })?;
    env.no_egress(c)
}

/// The error notifications and terminal status of one failing turn.
fn failing_turn(
    c: Check,
    stream: &mut Stream,
    session: &mut Session,
    mode: Mode,
    env: &ContractEnv,
) -> Result<(Vec<Value>, Value), crate::env::CaseFailure> {
    env.fake.queue([mode]);
    let (_, messages) = run_turn(c, stream, session, "this turn fails")?;
    let errors = messages
        .iter()
        .filter(|message| method_of(message) == Some("error"))
        .cloned()
        .collect();
    Ok((errors, messages.last().cloned().unwrap_or_default()))
}

pub fn error_notification_fatal(env: &ContractEnv) -> CaseResult {
    let c = Check("error_notification_fatal");
    let client = c.setup(env.launch(None, None), "starting the app-server")?;
    let mut session = c.ok(env.open(&client, Open::new()), "opening a tool-free thread")?;
    let mut stream = env.stream(&client);
    for (label, mode) in [
        ("HTTP 500", Mode::Http(500)),
        ("response.failed server_error", Mode::Failed("server_error")),
    ] {
        let (errors, done) = failing_turn(c, &mut stream, &mut session, mode, env)?;
        let error = c.some(
            errors.first(),
            &format!("an error notification for {label}"),
        )?;
        let parsed = Notification::parse(error);
        c.that(
            matches!(
                parsed,
                Notification::Error {
                    will_retry: false,
                    message: Some(_),
                    ..
                }
            ) && parsed.ends_turn(),
            || format!("{label}: the error notification read as {parsed:?}"),
        )?;
        let status = completed_status(&done);
        c.that(status.is_some_and(ProviderTurnStatus::is_failed), || {
            format!("{label}: the turn ended {done}, not with Codex's failed status")
        })?;
    }
    env.no_egress(c)
}

pub fn error_notification_capacity(env: &ContractEnv) -> CaseResult {
    let c = Check("error_notification_capacity");
    let client = c.setup(env.launch(None, None), "starting the app-server")?;
    let mut session = c.ok(env.open(&client, Open::new()), "opening a tool-free thread")?;
    let mut stream = env.stream(&client);
    let (errors, done) = failing_turn(
        c,
        &mut stream,
        &mut session,
        Mode::Failed("server_is_overloaded"),
        env,
    )?;
    let error = c.some(
        errors.first(),
        "an error notification for server_is_overloaded",
    )?;
    let message = match Notification::parse(error) {
        Notification::Error {
            message: Some(message),
            ..
        } => message.to_string(),
        other => return Err(c.fail(format!("the overload error read as {other:?}"))),
    };
    c.that(is_selected_model_capacity_error(&message), || {
        format!("Codex's capacity text is {message:?}, which is_selected_model_capacity_error does not match")
    })?;
    c.that(
        completed_status(&done).is_some_and(ProviderTurnStatus::is_failed),
        || {
            format!(
                "the capacity turn ended {done}, not with the failed status Cairn terminalizes on"
            )
        },
    )?;
    env.no_egress(c)
}

pub fn error_notification_will_retry(env: &ContractEnv) -> CaseResult {
    let c = Check("error_notification_will_retry");
    let client = c.setup(env.launch(None, None), "starting the app-server")?;
    let mut session = c.ok(env.open(&client, Open::new()), "opening a tool-free thread")?;
    let mut stream = env.stream(&client);
    env.fake.queue([Mode::Disconnect, Mode::Normal]);
    let (_, messages) = run_turn(c, &mut stream, &mut session, "retry once")?;
    let errors: Vec<Notification> = messages
        .iter()
        .filter(|message| method_of(message) == Some("error"))
        .map(Notification::parse)
        .collect();
    c.that(
        errors.iter().any(|error| {
            matches!(
                error,
                Notification::Error {
                    will_retry: true,
                    ..
                }
            ) && !error.ends_turn()
        }),
        || format!("no retrying error notification that leaves the turn running: {errors:?}"),
    )?;
    c.that(errors.iter().all(|error| !error.ends_turn()), || {
        format!("a recovered stream reported a fatal error: {errors:?}")
    })?;
    let done = messages.last().cloned().unwrap_or_default();
    c.that(
        completed_status(&done) == Some(ProviderTurnStatus::Completed),
        || format!("the retried turn ended {done}"),
    )?;
    env.no_egress(c)
}

pub fn mcp_tool_call_items(env: &ContractEnv) -> CaseResult {
    let c = Check("mcp_tool_call_items");
    let mcp = fixture_mcp();
    let client = c.setup(
        env.launch(Some(&mcp), None),
        "starting the app-server with the fixture MCP server",
    )?;
    let mut session = c.ok(
        env.open(&client, Open::new().mcp(mcp)),
        "opening a thread with the fixture attached",
    )?;
    let mut stream = env.stream(&client);
    env.fake.queue([
        Mode::Tool {
            needle: "echo",
            arguments: r#"{"text":"pong"}"#,
        },
        Mode::Normal,
    ]);
    let (_, messages) = run_turn(c, &mut stream, &mut session, "call echo")?;
    let served = env.fake.served();
    let offered = served
        .first()
        .map(|served| served.tools.clone())
        .unwrap_or_default();
    c.that(offered.iter().any(|tool| tool.contains("echo")), || {
        format!("Codex did not offer the fixture's echo tool to the model: {offered:?}")
    })?;
    let item = |method: &str| {
        messages.iter().find_map(|message| {
            let item = message.pointer("/params/item")?;
            (method_of(message) == Some(method) && item["type"] == "mcpToolCall")
                .then(|| item.clone())
        })
    };
    if item("item/started").is_none() {
        let kinds: Vec<String> = messages
            .iter()
            .filter_map(|message| {
                let kind = message.pointer("/params/item/type").and_then(Value::as_str);
                Some(format!("{} {}", method_of(message)?, kind.unwrap_or("")))
            })
            .collect();
        let modes: Vec<&str> = served.iter().map(|served| served.mode.as_str()).collect();
        return Err(c.fail(format!(
            "never observed an mcpToolCall item/started; the turn carried {kinds:?}; the provider answered {modes:?} offering {offered:?}"
        )));
    }
    let completed = c.some(item("item/completed"), "an mcpToolCall item/completed")?;
    let (text, is_error, _) = extract_mcp_result(&completed.get("result").cloned());
    c.that(text.contains("pong") && !is_error, || {
        format!("extract_mcp_result read {text:?} (error: {is_error}) from {completed}")
    })?;
    let mut mapper = EventMapper::new();
    let named = messages
        .iter()
        .flat_map(|message| mapper.map(message))
        .any(|event| matches!(&event, SessionEvent::ToolCall { name, .. } if name == "mcp__fixture__echo"));
    c.that(named, || {
        "EventMapper never named the call mcp__fixture__echo".to_string()
    })?;
    let unsupported = stream.unsupported_requests();
    c.that(unsupported.is_empty(), || {
        format!("Codex sent server requests Cairn has no handler for: {unsupported:?} (supported: {SUPPORTED_SERVER_REQUESTS:?})")
    })?;
    env.no_egress(c)
}

pub fn no_egress_at_startup(env: &ContractEnv) -> CaseResult {
    let c = Check("no_egress_at_startup");
    let mcp = fixture_mcp();
    let client = c.setup(env.launch(Some(&mcp), None), "starting the app-server")?;
    c.ok(
        env.open(&client, Open::new().mcp(mcp)),
        "opening an idle thread",
    )?;
    env.stream(&client).drain(Duration::from_secs(3));
    env.no_egress(c)
}

/// Every key and table name in a TOML document.
fn toml_keys(table: &toml::Table, into: &mut Vec<String>) {
    for (key, value) in table {
        into.push(key.clone());
        if let toml::Value::Table(inner) = value {
            toml_keys(inner, into);
        }
    }
}

/// Whether `text` names `key` as a whole word.
fn names_key(text: &str, key: &str) -> bool {
    let word = |c: char| c.is_ascii_alphanumeric() || c == '_';
    text.match_indices(key).any(|(at, _)| {
        let before = text[..at].chars().next_back();
        let after = text[at + key.len()..].chars().next();
        !before.is_some_and(word) && !after.is_some_and(word)
    })
}

pub fn no_ignored_config_warnings(env: &ContractEnv) -> CaseResult {
    let c = Check("no_ignored_config_warnings");
    let mcp = fixture_mcp();
    let client = c.setup(env.launch(Some(&mcp), None), "starting the app-server")?;
    c.ok(env.open(&client, Open::new().mcp(mcp)), "opening a thread")?;
    let mut stream = env.stream(&client);
    stream.drain(Duration::from_secs(2));
    let rendered = c.some(env.rendered_config(), "Cairn's rendered config.toml")?;
    let table: toml::Table = c.ok(toml::from_str(&rendered), "parsing Cairn's rendered config")?;
    let mut keys = Vec::new();
    toml_keys(&table, &mut keys);
    let mut ignored = Vec::new();
    for warning in stream
        .seen
        .iter()
        .filter(|message| method_of(message) == Some("configWarning"))
    {
        let text = format!(
            "{} {}",
            warning
                .pointer("/params/summary")
                .and_then(Value::as_str)
                .unwrap_or_default(),
            warning
                .pointer("/params/details")
                .and_then(Value::as_str)
                .unwrap_or_default()
        );
        for key in keys.iter().filter(|key| names_key(&text, key)) {
            ignored.push(format!("`{key}`: {text}"));
        }
    }
    c.that(ignored.is_empty(), || {
        format!("Codex warns about keys Cairn's hardening config sets, so a hardening is not in effect: {ignored:?}")
    })?;
    env.no_egress(c)
}

/// Start a thread signed in with an expired ChatGPT login against a backend
/// that rejects it, through Cairn's real dedicated `open()`: nothing answers a server
/// request until `thread/start` returns. Answers how long the open took and
/// the stream the requests are queued on.
fn expired_login_open(
    c: Check,
    env: &ContractEnv,
) -> Result<(Duration, Stream), crate::env::CaseFailure> {
    let mcp = fixture_mcp();
    let login = expired_chatgpt_login();
    let client = c.setup(
        env.launch(Some(&mcp), Some(&login)),
        "starting the app-server signed in with an expired ChatGPT token",
    )?;
    let started = Instant::now();
    let opened = env.open(&client, Open::new().mcp(mcp));
    let elapsed = started.elapsed();
    c.ok(
        opened,
        "opening a thread while Codex's token refresh goes unanswered",
    )
    .map_err(|failure| c.fail(format!("{} after {elapsed:?}", failure.detail)))?;
    Ok((elapsed, env.stream(&client)))
}

pub fn refresh_request_does_not_hold_thread_start(env: &ContractEnv) -> CaseResult {
    let c = Check("refresh_request_does_not_hold_thread_start");
    let (elapsed, _) = expired_login_open(c, env)?;
    c.that(elapsed < THREAD_START_BOUND, || {
        format!("thread/start took {elapsed:?} while its token refresh went unanswered; bound {THREAD_START_BOUND:?}")
    })?;
    env.only_remote_control_egress(c)
}

pub fn refresh_request_is_sent_with_expired_token(env: &ContractEnv) -> CaseResult {
    let c = Check("refresh_request_is_sent_with_expired_token");
    let (_, mut stream) = expired_login_open(c, env)?;
    stream.until(Duration::from_secs(30), |message| {
        matches!(
            Notification::parse(message),
            Notification::AuthTokensRefreshRequest { .. }
        )
    });
    c.that(
        stream.requests.iter().any(|method| method == "account/chatgptAuthTokens/refresh"),
        || {
            let methods: Vec<&str> = stream.seen.iter().filter_map(method_of).collect();
            format!(
                "Codex never asked for a token refresh with an expired login, so the thread/start bound tests nothing (requests: {:?}; messages: {methods:?}; egress: {:?})\ncodex stderr:\n{}",
                stream.requests,
                env.fake.egress(),
                env.stderr_tail()
            )
        },
    )?;
    env.only_remote_control_egress(c)
}

/// The requests Cairn's param builders produce, every option set.
fn builder_requests() -> Vec<(String, Value)> {
    let start = |tool_free, native| {
        build_thread_start_params(
            "/work",
            "never",
            "danger-full-access",
            Some(FAKE_MODEL),
            Some("low"),
            Some("priority"),
            "base",
            Some("developer"),
            tool_free,
            &["fixture".to_string()],
            native,
        )
    };
    let resume = || {
        build_thread_resume_params(
            "thread",
            "/work",
            "never",
            "danger-full-access",
            Some(FAKE_MODEL),
            Some("low"),
            Some("priority"),
            "base",
            Some("developer"),
        )
    };
    let fork = || {
        build_thread_fork_params(
            "thread",
            "/work",
            "never",
            "danger-full-access",
            Some(FAKE_MODEL),
            Some("low"),
            Some("priority"),
            "base",
            Some("developer"),
        )
    };
    vec![
        (
            "thread/start".to_string(),
            start(false, CodexNativeTools::None),
        ),
        (
            "thread/start".to_string(),
            start(true, CodexNativeTools::ImageGeneration),
        ),
        ("thread/resume".to_string(), resume()),
        ("thread/fork".to_string(), fork()),
    ]
}

fn generate_schema(c: Check, env: &ContractEnv) -> Result<Schema, crate::env::CaseFailure> {
    let codex = c.setup(env.managed().ensure(), "installing the pinned Codex")?;
    let out = env.dir().join("schema");
    c.ok(
        std::fs::create_dir_all(&out),
        "creating the schema directory",
    )?;
    c.ok(
        Schema::generate(&codex.path, &out),
        "generating the app-server JSON Schema",
    )
}

pub fn protocol_surface(env: &ContractEnv) -> CaseResult {
    protocol_surface_against(env, &Fixture::path())
}

pub fn protocol_surface_against(env: &ContractEnv, fixture: &std::path::Path) -> CaseResult {
    let c = Check("protocol_surface");
    let schema = generate_schema(c, env)?;
    let fixture = c.ok(Fixture::load(fixture), "reading the surface fixture")?;
    let mut problems = schema.check_fixture(&fixture);
    problems.extend(schema.check_requests(&builder_requests(), &fixture.known_dead));

    // What a live session actually wrote: initialize, thread/start, the MCP
    // attachment check, turn/start (first and later, with effort), the goal
    // read, and an interrupt.
    let mcp = fixture_mcp();
    let client = c.setup(env.launch(Some(&mcp), None), "starting the app-server")?;
    let mut session = c.ok(
        env.open(&client, Open::new().mcp(mcp).effort("low")),
        "opening a thread",
    )?;
    let mut stream = env.stream(&client);
    run_turn(c, &mut stream, &mut session, "first")?;
    run_turn(c, &mut stream, &mut session, "second")?;
    c.ok(session.control().get_goal(), "reading the goal")?;
    c.ok(
        session.control().interrupt(),
        "interrupting the finished turn",
    )?;
    let sent = sent_requests(&env.sent());
    c.that(
        sent.iter().any(|(method, _)| method == "turn/start"),
        || "the tap saw no turn/start".to_string(),
    )?;
    problems.extend(schema.check_requests(&sent, &fixture.known_dead));
    problems.sort();
    problems.dedup();
    c.that(problems.is_empty(), || {
        format!(
            "Codex {}'s protocol no longer matches what Cairn relies on:\n  {}",
            pin::embedded_pin().version,
            problems.join("\n  ")
        )
    })
}

/// The exemptions are narrow: a key nobody listed fails, and a key exempt on
/// one method is not exempt on another. Probed with a synthetic exemption, so
/// the property holds however many real `knownDead` entries the fixture has,
/// including none.
pub fn protocol_surface_exemptions_are_narrow(env: &ContractEnv) -> CaseResult {
    let c = Check("protocol_surface_exemptions_are_narrow");
    let schema = generate_schema(c, env)?;
    let known_dead = [KnownDead {
        method: "thread/resume".to_string(),
        key: "contractExemptKey".to_string(),
        issue: "cairn/0".to_string(),
    }];
    let probe = |method: &str, key: &str| {
        let mut params = match method {
            "turn/start" => json!({"threadId": "thread", "input": []}),
            _ => json!({"threadId": "thread"}),
        };
        params[key] = json!(true);
        schema.check_requests(&[(method.to_string(), params)], &known_dead)
    };
    let unlisted = probe("thread/resume", "contractNeverDefinedKey");
    c.that(
        unlisted
            .iter()
            .any(|problem| problem.contains("contractNeverDefinedKey")),
        || format!("an unlisted unknown key passed the request check: {unlisted:?}"),
    )?;
    let elsewhere = probe("turn/start", "contractExemptKey");
    c.that(
        elsewhere
            .iter()
            .any(|problem| problem.contains("contractExemptKey")),
        || format!("a key exempt only on thread/resume passed on turn/start: {elsewhere:?}"),
    )?;
    let exempt = probe("thread/resume", "contractExemptKey");
    c.that(exempt.is_empty(), || {
        format!("the listed resume exemption did not apply: {exempt:?}")
    })
}

/// [`protocol_surface`] against a fixture that reads a field Codex never had.
pub fn protocol_surface_with_a_phantom_read(env: &ContractEnv) -> CaseResult {
    let mut fixture: Value = serde_json::from_str(
        &std::fs::read_to_string(Fixture::path()).expect("the surface fixture is readable"),
    )
    .expect("the surface fixture is JSON");
    fixture["reads"]
        .as_array_mut()
        .expect("the fixture has reads")
        .push(
            json!({"type": "ErrorNotification", "path": "contractPhantomField", "required": true}),
        );
    let path = env.dir().join("phantom-surface.json");
    std::fs::write(&path, fixture.to_string()).expect("the scratch fixture is writable");
    protocol_surface_against(env, &path)
}
