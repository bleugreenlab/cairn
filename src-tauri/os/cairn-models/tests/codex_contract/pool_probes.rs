//! Probes for running ordinary Codex sessions as threads on one shared
//! app-server (cairn/4314, Milestone 0). Each drives the real pinned binary
//! with several threads on one server and records what it saw as a
//! `CODEX_POOL_PROBE` line on stderr, appended also to the file named by
//! `CAIRN_CODEX_PROBE_RECORD` when that is set.
//!
//! A probe fails only on a stop condition the design rests on. A probe whose
//! outcome chooses between designs (how long shims live, whether a thread
//! can bring its own MCP server) records its outcome and passes.

use std::sync::Arc;
use std::time::{Duration, Instant};

use cairn_models::codex::app_server::AppServerClient;
use cairn_models::interface::session::CodexNativeTools;
use cairn_models::session::codex::params::{
    build_thread_fork_params, build_thread_resume_params, build_thread_start_params,
};
use cairn_models::session::codex::thread::{loaded_threads, reconcile_goal, unsubscribe_thread};
use cairn_models::wire::codex_app_server::goal::GoalStatus;
use cairn_models::wire::codex_app_server::thread::ThreadUnsubscribeStatus;
use cairn_models::wire::codex_app_server::{build_codex_input, ProviderTurnStatus};
use cairn_models::wire::MessageContent;
use serde_json::{json, Value};

use crate::cases::{completed_status, run_turn, turn_completed, TURN};
use crate::env::{
    bun, fixture_mcp, method_of, thread_of, CaseFailure, CaseResult, Check, ContractEnv,
    FixtureLog, Open, Stream, FAKE_MODEL,
};
use crate::fake_responses::Mode;
use crate::proc_tree::ProcessTree;

/// A marker no other marker contains, routing a thread's provider requests
/// to its own queue.
fn marker(probe: &str, thread: &str) -> String {
    format!("[[{probe}-{thread}]]")
}

fn record(probe: &str, observed: Value) {
    let line = json!({ "probe": probe, "observed": observed }).to_string();
    eprintln!("CODEX_POOL_PROBE {line}");
    if let Some(path) = std::env::var_os("CAIRN_CODEX_PROBE_RECORD") {
        use std::io::Write;
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            let _ = writeln!(file, "{line}");
        }
    }
}

fn status_name(done: &Value) -> String {
    match completed_status(done) {
        Some(ProviderTurnStatus::Completed) => "completed".to_string(),
        Some(ProviderTurnStatus::Interrupted) => "interrupted".to_string(),
        Some(other) => format!("{other:?}"),
        None => format!("unread: {done}"),
    }
}

/// P1(a): a thread unsubscribed while still loaded resumes on the same
/// server as the same thread, and its next turn completes.
pub fn pool_resume_after_unsubscribe_while_loaded(env: &ContractEnv) -> CaseResult {
    let c = Check("pool_resume_after_unsubscribe_while_loaded");
    // A tool-free thread is ephemeral (no rollout), so these threads carry
    // the fixture, as every ordinary session carries Cairn's server.
    let mcp = fixture_mcp();
    let client = c.setup(env.launch(Some(&mcp), None), "starting the app-server")?;
    let mut stream = env.stream(&client);
    let mut first = c.ok(
        env.open(&client, Open::new().mcp(mcp.clone())),
        "opening a thread",
    )?;
    let thread = first.thread_id.clone();
    let mark = marker("p1", "a");
    env.fake.queue_for(
        &mark,
        [
            Mode::Reply("before".to_string()),
            Mode::Reply("after".to_string()),
        ],
    );
    run_turn(c, &mut stream, &mut first, &format!("{mark} first"))?;
    let status = c.ok(
        unsubscribe_thread(&client, &thread),
        "thread/unsubscribe on the idle thread",
    )?;
    let loaded = c.ok(loaded_threads(&client), "thread/loaded/list")?;
    let mut resumed = c.ok(
        env.open(&client, Open::new().mcp(mcp).resume(&thread)),
        "resuming the unsubscribed thread through Cairn's open",
    )?;
    let (_, messages) = run_turn(c, &mut stream, &mut resumed, "second")?;
    let done = messages.last().cloned().unwrap_or_default();
    let served: Vec<String> = env
        .fake
        .served()
        .into_iter()
        .filter(|served| served.marker.as_deref() == Some(mark.as_str()))
        .map(|served| served.mode)
        .collect();
    record(
        "P1a",
        json!({
            "unsubscribe": format!("{status:?}"),
            "loadedAfterUnsubscribe": loaded.contains(&thread),
            "resumedSameThread": resumed.thread_id == thread,
            "resumedTurn": status_name(&done),
            "providerRequests": served,
        }),
    );
    c.that(status == ThreadUnsubscribeStatus::Unsubscribed, || {
        format!("thread/unsubscribe answered {status:?}")
    })?;
    c.that(resumed.thread_id == thread, || {
        format!(
            "resuming {thread} after its unsubscribe fell back to a new thread {}",
            resumed.thread_id
        )
    })?;
    c.that(
        completed_status(&done) == Some(ProviderTurnStatus::Completed),
        || format!("the resumed thread's turn ended {done}"),
    )?;
    env.no_egress(c)
}

/// P2: interrupting a turn whose MCP tool call never answers ends that turn,
/// leaves the thread usable, and does not touch a neighbour on the server.
pub fn pool_interrupt_during_hung_mcp_call(env: &ContractEnv) -> CaseResult {
    let c = Check("pool_interrupt_during_hung_mcp_call");
    let (mcp, log) = env.logged_fixture("p2");
    let client = c.setup(env.launch(Some(&mcp), None), "starting the app-server")?;
    let mut stream = env.stream(&client);
    let mut hung = c.ok(
        env.open(&client, Open::new().mcp(mcp.clone())),
        "opening the thread that calls hang",
    )?;
    let mut neighbour = c.ok(
        env.open(&client, Open::new().mcp(mcp)),
        "opening the neighbour thread",
    )?;
    let (mark_hung, mark_neighbour) = (marker("p2", "hung"), marker("p2", "neighbour"));
    env.fake.queue_for(
        &mark_hung,
        [
            Mode::Tool {
                needle: "hang",
                arguments: r#"{"label":"p2"}"#,
            },
            Mode::Reply("after the hang".to_string()),
        ],
    );
    env.fake.queue_for(&mark_neighbour, [Mode::Slow]);

    let neighbour_turn = c.ok(
        neighbour.turn(&format!("{mark_neighbour} count slowly")),
        "starting the neighbour's slow turn",
    )?;
    let hung_turn = c.ok(
        hung.turn(&format!("{mark_hung} call hang")),
        "starting the turn that calls hang",
    )?;
    let call = c.some(
        log.until(TURN, |logged| {
            logged["event"] == "call" && logged["tool"] == "hang"
        }),
        "the hang tool receiving its call",
    )?;
    let shims_before = log.live_pids();

    let interrupted_at = Instant::now();
    c.ok(
        hung.control()
            .interrupt(cairn_models::interface::session::InterruptCause::OperatorStop),
        "CodexControl::interrupt while hang is in flight",
    )?;
    let done = c.some(
        stream.until(Duration::from_secs(20), turn_completed(&hung_turn)),
        "turn/completed after interrupting the hung tool call",
    )?;
    let interrupt_ms = interrupted_at.elapsed().as_millis() as u64;
    let cancelled = log.until(Duration::from_secs(3), |logged| {
        logged["event"] == "cancelled"
    });
    let neighbour_id = neighbour.thread_id.clone();
    let neighbour_streams = stream
        .until(Duration::from_secs(5), |message| {
            thread_of(message) == Some(neighbour_id.as_str())
                && method_of(message) == Some("item/agentMessage/delta")
        })
        .is_some();
    let neighbour_ended_early = stream
        .seen
        .iter()
        .any(|message| turn_completed(&neighbour_turn)(message));
    let server_tree = env
        .server_pid(&client)
        .map(ProcessTree::under)
        .transpose()
        .ok()
        .flatten();
    let shims_after: Vec<u64> = log.live_pids().into_iter().collect();
    let shims_in_tree = server_tree.as_ref().map(|tree| {
        shims_after
            .iter()
            .filter(|pid| tree.pids().contains(&(**pid as u32)))
            .count()
    });

    let (_, next) = run_turn(c, &mut stream, &mut hung, "again")?;
    let next_done = next.last().cloned().unwrap_or_default();
    c.ok(
        neighbour
            .control()
            .interrupt(cairn_models::interface::session::InterruptCause::OperatorStop),
        "interrupting the neighbour",
    )?;
    let neighbour_done = stream.until(Duration::from_secs(20), turn_completed(&neighbour_turn));

    record(
        "P2",
        json!({
            "hangCall": call,
            "interruptedTurn": status_name(&done),
            "interruptToCompletedMs": interrupt_ms,
            "cancelledNotification": cancelled,
            "shimsBefore": shims_before,
            "shimsAfter": shims_after,
            "shimsStillUnderServer": shims_in_tree,
            "neighbourKeptStreaming": neighbour_streams,
            "neighbourEndedEarly": neighbour_ended_early,
            "nextTurnOnInterruptedThread": status_name(&next_done),
            "neighbourAfterItsOwnInterrupt": neighbour_done.as_ref().map(status_name),
        }),
    );
    c.that(
        completed_status(&done) == Some(ProviderTurnStatus::Interrupted),
        || format!("the turn with hang in flight ended {done}"),
    )?;
    c.that(neighbour_streams && !neighbour_ended_early, || {
        "the neighbour's turn stopped streaming when its neighbour was interrupted".to_string()
    })?;
    c.that(
        completed_status(&next_done) == Some(ProviderTurnStatus::Completed),
        || format!("the next turn on the interrupted thread ended {next_done}"),
    )?;
    env.no_egress(c)
}

#[cfg(unix)]
fn alive(pid: u64) -> bool {
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

/// Poll the attached child for exit, which also reaps it.
fn exits_within(client: &Arc<AppServerClient>, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    loop {
        let exited = client
            .child_handle()
            .lock()
            .ok()
            .and_then(|mut child| child.as_mut().map(|child| child.try_wait()))
            .map(|status| matches!(status, Ok(Some(_))))
            .unwrap_or(true);
        if exited {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// A server with two fixture-carrying threads that have each run a turn.
fn two_thread_server(
    c: Check,
    env: &ContractEnv,
    name: &str,
) -> Result<(Arc<AppServerClient>, FixtureLog, Vec<String>), CaseFailure> {
    let (mcp, log) = env.logged_fixture(name);
    let client = c.setup(env.launch(Some(&mcp), None), "starting the app-server")?;
    let mut stream = env.stream(&client);
    let mut threads = Vec::new();
    for which in ["a", "b"] {
        let mut session = c.ok(
            env.open(&client, Open::new().mcp(mcp.clone())),
            "opening a fixture thread",
        )?;
        run_turn(
            c,
            &mut stream,
            &mut session,
            &format!("{} hi", marker(name, which)),
        )?;
        threads.push(session.thread_id.clone());
    }
    Ok((client, log, threads))
}

/// P4: how many MCP shims a server runs per thread, and which of them
/// outlive an unsubscribe, a SIGTERM to the server pid alone, and the
/// server's stdin closing (the runner dying). Recorded, not asserted: a
/// surviving shim means a server restart must signal the whole group.
#[cfg(unix)]
pub fn pool_shim_lifetime(env: &ContractEnv) -> CaseResult {
    let c = Check("pool_shim_lifetime");
    let survivors = |log: &FixtureLog| -> Vec<u64> {
        log.named("start")
            .iter()
            .filter_map(|logged| logged["pid"].as_u64())
            .filter(|pid| alive(*pid))
            .collect()
    };

    let (client, log, threads) = two_thread_server(c, env, "p4term")?;
    let pid = c.some(env.server_pid(&client), "the app-server's pid")?;
    let after_start = log.live_pids();
    let tree = c.ok(ProcessTree::under(pid), "reading the server's process tree")?;
    c.ok(
        unsubscribe_thread(&client, &threads[0]),
        "thread/unsubscribe on one thread",
    )?;
    std::thread::sleep(Duration::from_secs(2));
    let after_unsubscribe = log.live_pids();
    unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
    let server_exited = exits_within(&client, Duration::from_secs(10));
    std::thread::sleep(Duration::from_secs(1));
    let after_sigterm = survivors(&log);
    for pid in &after_sigterm {
        unsafe { libc::kill(*pid as libc::pid_t, libc::SIGKILL) };
    }

    let (eof_client, eof_log, _) = two_thread_server(c, env, "p4eof")?;
    let eof_before = eof_log.live_pids();
    eof_client.close_stdin();
    let eof_exited = exits_within(&eof_client, Duration::from_secs(10));
    std::thread::sleep(Duration::from_secs(1));
    let after_eof = survivors(&eof_log);
    for pid in &after_eof {
        unsafe { libc::kill(*pid as libc::pid_t, libc::SIGKILL) };
    }

    record(
        "P4",
        json!({
            "threads": threads.len(),
            "shimsAfterStart": after_start,
            "tree": tree.render(),
            "shimsAfterUnsubscribingOne": after_unsubscribe,
            "serverExitedOnSigterm": server_exited,
            "shimsSurvivingSigterm": after_sigterm,
            "shimsBeforeStdinEof": eof_before,
            "serverExitedOnStdinEof": eof_exited,
            "shimsSurvivingStdinEof": after_eof,
        }),
    );
    c.that(server_exited && eof_exited, || {
        format!("the server outlived SIGTERM ({server_exited}) or stdin EOF ({eof_exited})")
    })
}

/// P6: `_meta.threadId` on a tool call comes from Codex, and a model that
/// writes `_meta` or `threadId` into its arguments does not change it.
pub fn pool_tool_call_thread_id_comes_from_codex(env: &ContractEnv) -> CaseResult {
    let c = Check("pool_tool_call_thread_id_comes_from_codex");
    let (mcp, log) = env.logged_fixture("p6");
    let client = c.setup(env.launch(Some(&mcp), None), "starting the app-server")?;
    let mut stream = env.stream(&client);
    let mut session = c.ok(env.open(&client, Open::new().mcp(mcp)), "opening a thread")?;
    let mark = marker("p6", "a");
    env.fake.queue_for(
        &mark,
        [
            Mode::Tool {
                needle: "echo",
                arguments: r#"{"text":"p6","_meta":{"threadId":"forged-thread"},"threadId":"forged-thread"}"#,
            },
            Mode::Normal,
        ],
    );
    run_turn(c, &mut stream, &mut session, &format!("{mark} call echo"))?;
    let call = c.some(
        log.until(Duration::from_secs(5), |logged| {
            logged["event"] == "call" && logged["tool"] == "echo"
        }),
        "the echo tool receiving its call",
    )?;
    let meta = call.pointer("/params/_meta").cloned();
    let carried = call
        .pointer("/params/_meta/threadId")
        .and_then(Value::as_str)
        .map(str::to_string);
    record(
        "P6",
        json!({
            "thread": session.thread_id,
            "meta": meta,
            "arguments": call.pointer("/params/arguments"),
        }),
    );
    c.that(carried.as_deref() == Some(session.thread_id.as_str()), || {
        format!(
            "the tool call's _meta was {meta:?}; Cairn attributes a pooled call by _meta.threadId, which must be the calling thread {}",
            session.thread_id
        )
    })?;
    env.no_egress(c)
}

/// The per-thread `config` that adds the logging fixture as a thread's own
/// MCP server.
fn fixture_thread_config(log: &FixtureLog) -> Value {
    json!({
        "mcp_servers.fixture.command": bun(),
        "mcp_servers.fixture.args": [
            format!("{}/tests/codex_contract/mcp_fixture.ts", env!("CARGO_MANIFEST_DIR")),
            log.path().to_string_lossy(),
        ],
    })
}

fn with_config(mut params: Value, config: &Value) -> Value {
    let merged = params
        .get("config")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .chain(config.as_object().cloned().unwrap_or_default())
        .collect::<serde_json::Map<String, Value>>();
    params["config"] = Value::Object(merged);
    params
}

/// Run one raw turn on `thread` that asks for the fixture's echo, and say
/// whether the provider was offered it and the fixture answered it.
fn echo_turn(
    c: Check,
    env: &ContractEnv,
    client: &Arc<AppServerClient>,
    stream: &mut Stream,
    log: &FixtureLog,
    thread: &str,
    mark: &str,
) -> Result<Value, CaseFailure> {
    env.fake.queue_for(
        mark,
        [
            Mode::Tool {
                needle: "echo",
                arguments: r#"{"text":"p7"}"#,
            },
            Mode::Normal,
        ],
    );
    let calls_before = log.named("call").len();
    let input = c.ok(
        build_codex_input(&MessageContent::text(&format!("{mark} call echo"))),
        "building the turn input",
    )?;
    let started = c.ok(
        client.send_request("turn/start", json!({ "threadId": thread, "input": input })),
        "turn/start",
    )?;
    let turn = c.some(
        started
            .pointer("/turn/id")
            .and_then(Value::as_str)
            .map(str::to_string),
        "turn/start's turn id",
    )?;
    let done = c.some(stream.until(TURN, turn_completed(&turn)), "turn/completed")?;
    let offered = env
        .fake
        .served()
        .into_iter()
        .find(|served| served.marker.as_deref() == Some(mark))
        .map(|served| served.tools)
        .unwrap_or_default();
    Ok(json!({
        "offeredEcho": offered.iter().any(|tool| tool.contains("echo")),
        "fixtureAnswered": log.named("call").len() > calls_before,
        "turn": status_name(&done),
    }))
}

fn opened_thread(result: &Value) -> Option<String> {
    result
        .pointer("/thread/id")
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// P7: a thread on a server started with no MCP server brings the fixture
/// in through its own `config`, on start, resume and fork. Recorded, not
/// asserted: if it cannot, the MCP set belongs in the pool key.
pub fn pool_thread_brings_its_own_mcp_server(env: &ContractEnv) -> CaseResult {
    let c = Check("pool_thread_brings_its_own_mcp_server");
    let (_, log) = env.logged_fixture("p7");
    let config = fixture_thread_config(&log);
    let client = c.setup(
        env.launch(None, None),
        "starting an app-server with no MCP server",
    )?;
    let mut stream = env.stream(&client);
    let cwd = env.dir().join("work").to_string_lossy().into_owned();
    let base = "You are the codex-contract test agent.";

    let start = with_config(
        build_thread_start_params(
            &cwd,
            "never",
            "danger-full-access",
            Some(FAKE_MODEL),
            None,
            None,
            base,
            None,
            false,
            &[],
            CodexNativeTools::None,
        ),
        &config,
    );
    let started = client.send_request("thread/start", start);
    let thread = match started.as_ref().ok().and_then(opened_thread) {
        Some(thread) => thread,
        None => {
            record("P7", json!({ "start": format!("refused: {started:?}") }));
            return env.no_egress(c);
        }
    };
    let on_start = echo_turn(
        c,
        env,
        &client,
        &mut stream,
        &log,
        &thread,
        &marker("p7", "start"),
    )?;

    c.ok(unsubscribe_thread(&client, &thread), "thread/unsubscribe")?;
    let resume = with_config(
        build_thread_resume_params(
            &thread,
            &cwd,
            "never",
            "danger-full-access",
            Some(FAKE_MODEL),
            None,
            None,
            base,
            None,
        ),
        &config,
    );
    let on_resume = match client.send_request("thread/resume", resume) {
        Ok(_) => echo_turn(
            c,
            env,
            &client,
            &mut stream,
            &log,
            &thread,
            &marker("p7", "resume"),
        )?,
        Err(error) => json!({ "refused": error }),
    };

    let fork = with_config(
        build_thread_fork_params(
            &thread,
            &cwd,
            "never",
            "danger-full-access",
            Some(FAKE_MODEL),
            None,
            None,
            base,
            None,
        ),
        &config,
    );
    let on_fork = match client.send_request("thread/fork", fork) {
        Ok(result) => match opened_thread(&result) {
            Some(forked) => echo_turn(
                c,
                env,
                &client,
                &mut stream,
                &log,
                &forked,
                &marker("p7", "fork"),
            )?,
            None => json!({ "noThreadId": result }),
        },
        Err(error) => json!({ "refused": error }),
    };

    record(
        "P7",
        json!({
            "start": on_start,
            "resume": on_resume,
            "fork": on_fork,
            "shimsStarted": log.named("start").len(),
        }),
    );
    env.no_egress(c)
}

/// The provider requests served across `period`, before and after. A goal's
/// continuation requests do not repeat its objective, so a marker cannot
/// count them: a case using this runs one thread on its own provider.
fn still_running(env: &ContractEnv, period: Duration) -> (usize, usize) {
    let before = env.fake.served().len();
    std::thread::sleep(period);
    (before, env.fake.served().len())
}

/// P3: a native goal on a shared server. (a) Does an active goal keep
/// starting turns once its thread is unsubscribed? (b) Can its goal be
/// paused while unsubscribed, and does resuming the now-paused thread run no
/// turn by itself? The resume also carries `deferGoalContinuation`, which
/// upstream declares on fork only, so this establishes nothing about an
/// active goal: that case, across a server's loss, is
/// `pool_active_goal_paused_before_resume_after_server_loss`. (c) Do pause,
/// interrupt and unsubscribe, in that order, leave nothing running? Turns
/// are counted by provider requests, since an unsubscribed thread sends its
/// client nothing.
pub fn pool_goal_on_a_shared_server(env: &ContractEnv) -> CaseResult {
    let c = Check("pool_goal_on_a_shared_server");
    let mcp = fixture_mcp();
    let client = c.setup(env.launch(Some(&mcp), None), "starting the app-server")?;
    let mut stream = env.stream(&client);
    let mut session = c.ok(env.open(&client, Open::new().mcp(mcp)), "opening a thread")?;
    let thread = session.thread_id.clone();
    let mark = marker("p3", "goal");
    let objective = format!("{mark} keep working; never mark this goal complete");

    c.ok(
        session
            .control()
            .set_goal(Some(&objective), Some(GoalStatus::Active), None),
        "setting an active goal",
    )?;
    let mut completions = 0;
    c.some(
        stream.until(TURN, |message| {
            if method_of(message) == Some("turn/completed")
                && thread_of(message) == Some(thread.as_str())
            {
                completions += 1;
            }
            completions >= 2
        }),
        "two goal continuation turns",
    )?;

    c.ok(
        unsubscribe_thread(&client, &thread),
        "thread/unsubscribe with the goal active",
    )?;
    let (a_before, a_after) = still_running(env, Duration::from_secs(3));

    let paused = session
        .control()
        .set_goal(None, Some(GoalStatus::Paused), None)
        .map(|goal| format!("{:?}", goal.status));
    let interrupted = session
        .control()
        .interrupt(cairn_models::interface::session::InterruptCause::OperatorStop);
    let unsubscribed_again = unsubscribe_thread(&client, &thread).map(|s| format!("{s:?}"));
    std::thread::sleep(Duration::from_secs(1));
    let (c_before, c_after) = still_running(env, Duration::from_secs(5));

    let cwd = env.dir().join("work").to_string_lossy().into_owned();
    let mut resume = build_thread_resume_params(
        &thread,
        &cwd,
        "never",
        "danger-full-access",
        Some(FAKE_MODEL),
        None,
        None,
        "You are the codex-contract test agent.",
        None,
    );
    resume["deferGoalContinuation"] = json!(true);
    let resumed = client
        .send_request("thread/resume", resume)
        .map(|_| "resumed".to_string());
    let (b_before, b_after) = still_running(env, Duration::from_secs(3));
    let goal_after_resume = session
        .control()
        .get_goal()
        .map(|goal| goal.map(|goal| format!("{:?}", goal.status)));
    let _ = reconcile_goal(&client, &thread, None, true);

    record(
        "P3",
        json!({
            "a_requestsAcrossThreeSecondsAfterUnsubscribe": [a_before, a_after],
            "a_continuesAfterUnsubscribe": a_after > a_before,
            "c_pauseWhileUnsubscribed": format!("{paused:?}"),
            "c_interrupt": format!("{interrupted:?}"),
            "c_unsubscribeAgain": format!("{unsubscribed_again:?}"),
            "c_requestsAcrossFiveSecondsAfterPauseInterruptUnsubscribe": [c_before, c_after],
            "b_resumeOfPausedGoal": format!("{resumed:?}"),
            "b_requestsAcrossThreeSecondsAfterResumingPaused": [b_before, b_after],
            "b_goalAfterResume": format!("{goal_after_resume:?}"),
        }),
    );
    c.that(paused.is_ok(), || {
        format!("the goal could not be paused on an unsubscribed, loaded thread: {paused:?}")
    })?;
    c.that(c_after == c_before, || {
        format!("turns kept starting after pause, interrupt and unsubscribe ({c_before} then {c_after} requests)")
    })?;
    c.that(b_after == b_before, || {
        format!("resuming a paused-goal thread started a turn by itself ({b_before} then {b_after} requests)")
    })?;
    env.no_egress(c)
}

fn percentile(sorted_ms: &[u64], fraction: f64) -> u64 {
    if sorted_ms.is_empty() {
        return 0;
    }
    let index = ((sorted_ms.len() as f64 - 1.0) * fraction).round() as usize;
    sorted_ms[index]
}

fn latency_summary(mut ms: Vec<u64>) -> Value {
    ms.sort_unstable();
    json!({
        "n": ms.len(),
        "p50Ms": percentile(&ms, 0.5),
        "p99Ms": percentile(&ms, 0.99),
        "maxMs": ms.last().copied().unwrap_or(0),
    })
}

fn mib_of(bytes: u64) -> f64 {
    (bytes as f64 / (1024.0 * 1024.0) * 10.0).round() / 10.0
}

/// Run one turn on every session at once, and answer each thread's
/// streamed text.
fn concurrent_turns(
    c: Check,
    stream: &mut Stream,
    sessions: &mut [crate::env::Session],
    marks: &[String],
    text: &str,
) -> Result<std::collections::HashMap<String, String>, CaseFailure> {
    let mut turns = std::collections::HashSet::new();
    for (session, mark) in sessions.iter_mut().zip(marks) {
        turns.insert(c.ok(
            session.turn(&format!("{mark} {text}")),
            "starting a concurrent turn",
        )?);
    }
    let mut texts: std::collections::HashMap<String, String> = Default::default();
    let mut left = turns.len();
    c.some(
        stream.until(Duration::from_secs(300), |message| {
            let thread = thread_of(message).unwrap_or_default().to_string();
            match method_of(message) {
                Some("item/agentMessage/delta") => {
                    if let Some(delta) = message.pointer("/params/delta").and_then(Value::as_str) {
                        texts.entry(thread).or_default().push_str(delta);
                    }
                }
                Some("turn/completed") => {
                    if turns.iter().any(|turn| turn_completed(turn)(message)) {
                        left -= 1;
                    }
                }
                _ => {}
            }
            left == 0
        }),
        "every concurrent turn completing within 300 s",
    )?;
    Ok(texts)
}

/// P8: many threads, each with its own fixture shim, on one server. Sizes
/// the shard limit K: footprint (server, code-mode host, shims), open
/// latency, routing of concurrent turns, and the time to resume every
/// thread on a fresh server one at a time, as the resume permit would.
/// `CAIRN_CODEX_POOL_LOAD` sets the thread count (20 by default, so the
/// lane stays quick; the plan's measurement is 100).
pub fn pool_load(env: &ContractEnv) -> CaseResult {
    let c = Check("pool_load");
    let n: usize = std::env::var("CAIRN_CODEX_POOL_LOAD")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(20);
    let (mcp, log) = env.logged_fixture("p8");
    let client = c.setup(env.launch(Some(&mcp), None), "starting the app-server")?;
    let pid = c.some(env.server_pid(&client), "the app-server's pid")?;
    let idle = c.ok(ProcessTree::under(pid), "reading the idle server's tree")?;
    let mut stream = env.stream(&client);

    let mut sessions = Vec::with_capacity(n);
    let mut marks = Vec::with_capacity(n);
    let mut opens = Vec::with_capacity(n);
    for i in 0..n {
        let mark = marker("p8", &format!("{i:03}"));
        env.fake.queue_for(
            &mark,
            [
                Mode::Reply(format!("first-{i:03}")),
                Mode::Reply(format!("second-{i:03}")),
            ],
        );
        let started = Instant::now();
        sessions.push(c.ok(
            env.open(&client, Open::new().mcp(mcp.clone())),
            "opening a load thread",
        )?);
        opens.push(started.elapsed().as_millis() as u64);
        marks.push(mark);
    }
    let opened = c.ok(ProcessTree::under(pid), "reading the loaded server's tree")?;

    let started = Instant::now();
    let first = concurrent_turns(c, &mut stream, &mut sessions, &marks, "first")?;
    let first_ms = started.elapsed().as_millis() as u64;
    let second = concurrent_turns(c, &mut stream, &mut sessions, &marks, "second")?;
    let misrouted: Vec<String> = sessions
        .iter()
        .enumerate()
        .filter(|(i, session)| {
            first.get(&session.thread_id) != Some(&format!("first-{i:03}"))
                || second.get(&session.thread_id) != Some(&format!("second-{i:03}"))
        })
        .map(|(_, session)| session.thread_id.clone())
        .collect();
    let busy = c.ok(
        ProcessTree::under(pid),
        "reading the server's tree after the turns",
    )?;
    let server = busy.processes.first().and_then(|root| root.footprint_bytes);
    let mut children: std::collections::BTreeMap<String, (usize, u64)> = Default::default();
    for child in busy.processes.iter().filter(|process| process.pid != pid) {
        let entry = children.entry(child.command.clone()).or_default();
        entry.0 += 1;
        entry.1 += child.footprint_bytes.unwrap_or(0);
    }
    let children: Value = children
        .into_iter()
        .map(|(command, (count, bytes))| {
            (
                command,
                json!({ "count": count, "footprintMiB": mib_of(bytes) }),
            )
        })
        .collect::<serde_json::Map<String, Value>>()
        .into();

    client.shutdown();
    exits_within(&client, Duration::from_secs(10));
    let successor = c.setup(env.launch(Some(&mcp), None), "starting the successor")?;
    let mut resumes = Vec::with_capacity(n);
    let mut not_resumed = Vec::new();
    let resuming = Instant::now();
    for session in &sessions {
        let started = Instant::now();
        match env.open(
            &successor,
            Open::new().mcp(mcp.clone()).resume(&session.thread_id),
        ) {
            Ok(resumed) if resumed.thread_id == session.thread_id => {}
            Ok(resumed) => not_resumed.push(format!(
                "{} fell back to {}",
                session.thread_id, resumed.thread_id
            )),
            Err(error) => not_resumed.push(format!("{}: {error}", session.thread_id)),
        }
        resumes.push(started.elapsed().as_millis() as u64);
    }
    let resume_all_ms = resuming.elapsed().as_millis() as u64;

    record(
        "P8",
        json!({
            "threads": n,
            "idleServerFootprintMiB": idle.processes.first().and_then(|root| root.footprint_bytes).map(mib_of),
            "openedTreeFootprintMiB": mib_of(opened.footprint_bytes()),
            "serverFootprintMiBAfterTwoTurnsEach": server.map(mib_of),
            "childrenByCommand": children,
            "treeFootprintMiB": mib_of(busy.footprint_bytes()),
            "processesAppearedWhileOpening": idle.diff(&opened).appeared.len(),
            "shimsLogged": log.named("start").len(),
            "open": latency_summary(opens),
            "firstConcurrentTurnsMs": first_ms,
            "misrouted": misrouted,
            "resumeOnSuccessor": latency_summary(resumes),
            "resumeAllMs": resume_all_ms,
            "notResumed": not_resumed,
        }),
    );
    c.that(misrouted.is_empty(), || {
        format!(
            "{} of {n} threads received another thread's reply: {misrouted:?}",
            misrouted.len()
        )
    })?;
    c.that(not_resumed.is_empty(), || {
        format!("threads did not resume on the successor: {not_resumed:?}")
    })?;
    env.no_egress(c)
}

/// Start a thread with raw `thread/start` params plus `config`, as Cairn
/// would on a shared server, and answer its id.
fn raw_thread(
    c: Check,
    env: &ContractEnv,
    client: &Arc<AppServerClient>,
    config: Value,
) -> Result<String, CaseFailure> {
    let cwd = env.dir().join("work").to_string_lossy().into_owned();
    let params = with_config(
        build_thread_start_params(
            &cwd,
            "never",
            "danger-full-access",
            Some(FAKE_MODEL),
            None,
            None,
            "You are the codex-contract test agent.",
            None,
            false,
            client.mcp_servers(),
            CodexNativeTools::None,
        ),
        &config,
    );
    let started = c.ok(client.send_request("thread/start", params), "thread/start")?;
    c.some(opened_thread(&started), "thread/start's thread id")
}

/// Run one raw turn carrying `mark` with the provider answering `modes`,
/// and answer the requests it served for the turn.
fn raw_turn(
    c: Check,
    env: &ContractEnv,
    client: &Arc<AppServerClient>,
    stream: &mut Stream,
    thread: &str,
    mark: &str,
    modes: Vec<Mode>,
) -> Result<Vec<crate::fake_responses::Served>, CaseFailure> {
    env.fake.queue_for(mark, modes);
    let input = c.ok(
        build_codex_input(&MessageContent::text(&format!("{mark} go"))),
        "building the turn input",
    )?;
    let started = c.ok(
        client.send_request("turn/start", json!({ "threadId": thread, "input": input })),
        "turn/start",
    )?;
    let turn = c.some(
        started
            .pointer("/turn/id")
            .and_then(Value::as_str)
            .map(str::to_string),
        "turn/start's turn id",
    )?;
    c.some(stream.until(TURN, turn_completed(&turn)), "turn/completed")?;
    Ok(env
        .fake
        .served()
        .into_iter()
        .filter(|served| served.marker.as_deref() == Some(mark))
        .collect())
}

/// What model code inside code mode reports about its reach.
const CODE_MODE_REACH: &str = r#"const r = {};
for (const name of ["process", "require", "fetch", "Deno", "Bun", "XMLHttpRequest", "WebSocket"]) {
  r[name] = typeof globalThis[name];
}
try { r.env = Object.keys(process.env); } catch (e) { r.env = "refused: " + String(e).slice(0, 80); }
for (const spec of ["node:child_process", "node:fs", "child_process", "fs", "https://example.invalid/x.js"]) {
  try { await import(spec); r["import " + spec] = "loaded"; }
  catch (e) { r["import " + spec] = "refused: " + String(e).slice(0, 80); }
}
r.globals = Object.getOwnPropertyNames(globalThis).sort().join(",");
text("CODE_MODE_REACH " + JSON.stringify(r));"#;

/// P5 (security condition 2): nothing an agent controls makes the shared
/// server start a process. (a) A worktree's own `.codex/config.toml` and
/// `.codex/hooks.json`, an agent can write both, plant an MCP server and a
/// command hook on every event; neither may run, nor the planted server be
/// offered. (b) With code mode switched on for one thread, model code must
/// reach no process, file system, network, module or environment. Any
/// reach is a stop: the build goes back to the security thread.
#[cfg(unix)]
pub fn pool_agent_cannot_spawn_under_the_server(env: &ContractEnv) -> CaseResult {
    let c = Check("pool_agent_cannot_spawn_under_the_server");
    let work = env.dir().join("work");
    let hook_marker = env.dir().join("hook-ran");
    let (_, planted_log) = env.logged_fixture("p5-planted");
    let hook = format!("echo $$ >> '{}'", hook_marker.display());
    let fixture = format!(
        "{}/tests/codex_contract/mcp_fixture.ts",
        env!("CARGO_MANIFEST_DIR")
    );
    let planted_config = format!(
        "[mcp_servers.planted]\ncommand = {bun:?}\nargs = [{fixture:?}, {log:?}]\n\n[features]\nshell_tool = true\nunified_exec = true\n",
        bun = bun(),
        log = planted_log.path().to_string_lossy(),
    );
    let events = [
        "SessionStart",
        "UserPromptSubmit",
        "PreToolUse",
        "PostToolUse",
        "Stop",
    ];
    let hooks_json = json!({
        "hooks": events
            .iter()
            .map(|event| (event.to_string(), json!([{ "hooks": [{ "type": "command", "command": hook }] }])))
            .collect::<serde_json::Map<String, Value>>(),
    });
    let planted = c.setup(
        (|| -> std::io::Result<()> {
            std::fs::create_dir_all(work.join(".git"))?;
            std::fs::create_dir_all(work.join(".codex"))?;
            std::fs::write(work.join(".codex/config.toml"), &planted_config)?;
            std::fs::write(work.join(".codex/hooks.json"), hooks_json.to_string())?;
            std::fs::write(work.join("config.toml"), &planted_config)
        })(),
        "planting config and hooks in the worktree",
    );
    planted?;

    let (mcp, log) = env.logged_fixture("p5");
    let client = c.setup(env.launch(Some(&mcp), None), "starting the app-server")?;
    let pid = c.some(env.server_pid(&client), "the app-server's pid")?;
    let mut stream = env.stream(&client);
    let mut session = c.ok(
        env.open(&client, Open::new().mcp(mcp)),
        "opening a thread in the planted worktree",
    )?;
    let mark = marker("p5", "planted");
    env.fake.queue_for(
        &mark,
        [
            Mode::Tool {
                needle: "echo",
                arguments: r#"{"text":"p5"}"#,
            },
            Mode::Normal,
        ],
    );
    run_turn(c, &mut stream, &mut session, &format!("{mark} call echo"))?;
    std::thread::sleep(Duration::from_secs(2));
    let offered: Vec<String> = env
        .fake
        .served()
        .into_iter()
        .filter(|served| served.marker.as_deref() == Some(mark.as_str()))
        .flat_map(|served| served.tools)
        .collect();
    let planted_offered: Vec<&String> = offered
        .iter()
        .filter(|tool| {
            tool.contains("planted") || tool.contains("shell") || tool.contains("exec_command")
        })
        .collect();
    let hook_ran = std::fs::read_to_string(&hook_marker).ok();
    let planted_started = planted_log.named("start");
    let after_planted = c.ok(ProcessTree::under(pid), "reading the server's tree")?;

    // (b) code mode, switched on for one thread.
    let default_offers_exec = offered.iter().any(|tool| tool == "exec");
    let thread = raw_thread(c, env, &client, json!({ "features.code_mode": true }))?;
    let served = raw_turn(
        c,
        env,
        &client,
        &mut stream,
        &thread,
        &marker("p5", "code"),
        vec![
            Mode::Custom {
                name: "exec",
                input: CODE_MODE_REACH.to_string(),
            },
            Mode::Normal,
        ],
    )?;
    let code_mode_offered = served
        .first()
        .is_some_and(|served| served.tools.iter().any(|tool| tool == "exec"));
    let reach = served.iter().find_map(|served| {
        let at = served.input_tail.find("CODE_MODE_REACH ")?;
        Some(served.input_tail[at..].to_string())
    });
    let after_code = c.ok(
        ProcessTree::under(pid),
        "reading the server's tree after code mode",
    )?;
    let appeared: Vec<String> = after_planted
        .diff(&after_code)
        .appeared
        .iter()
        .map(|process| format!("{} {}", process.pid, process.command))
        .collect();
    let shims: Vec<u64> = log
        .named("start")
        .iter()
        .filter_map(|l| l["pid"].as_u64())
        .collect();
    // Codex's own code-mode host is expected once: (b) switched code mode on,
    // and its reach is what (b) checks. Anything else, or a second host, is
    // a process an agent caused.
    let mut hosts = 0;
    let unexpected: Vec<String> = after_code
        .processes
        .iter()
        .filter(|process| process.pid != pid && !shims.contains(&(process.pid as u64)))
        .filter(|process| {
            // Linux reports a name cut to 15 bytes: `codex-code-mode`.
            let host = process.command.starts_with("codex-code-mode");
            hosts += usize::from(host);
            !(host && hosts == 1)
        })
        .map(|process| format!("{} {}", process.pid, process.command))
        .collect();

    record(
        "P5",
        json!({
            "a_hookRan": hook_ran,
            "a_plantedServerStarted": planted_started,
            "a_plantedToolsOffered": planted_offered,
            "a_toolsOffered": offered,
            "b_defaultOffersExec": default_offers_exec,
            "b_codeModeOfferedWhenEnabled": code_mode_offered,
            "b_reach": reach,
            "b_processesAppearedDuringCodeMode": appeared,
            "childrenOtherThanFixtureShims": unexpected,
            "tree": after_code.render(),
        }),
    );
    c.that(hook_ran.is_none(), || {
        format!("a hook planted in the worktree ran: {hook_ran:?}")
    })?;
    c.that(planted_started.is_empty() && planted_offered.is_empty(), || {
        format!("the worktree's planted config took effect: started {planted_started:?}, offered {planted_offered:?}")
    })?;
    if let Some(reach) = &reach {
        let loaded = reach.contains("\\\"loaded\\\"") || reach.contains("\"loaded\"");
        let has_process = !reach.contains("\"process\":\"undefined\"")
            && !reach.contains("\\\"process\\\":\\\"undefined\\\"");
        c.that(!loaded && !has_process, || {
            format!("model code in code mode reached past its sandbox: {reach}")
        })?;
    }
    c.that(unexpected.is_empty(), || {
        format!("processes other than the fixture shims ran under the server: {unexpected:?}")
    })?;
    env.no_egress(c)
}

/// P1(b): a thread unsubscribed with its goal already paused is unloaded by
/// Codex after its unload delay, then resumes on the same server as the same
/// thread, with its goal still paused and no turn started by the resume
/// itself. Also records whether the goal can be set while the thread is
/// unloaded. An active goal across a resume is
/// `pool_active_goal_paused_before_resume_after_server_loss`. Waits up to 40
/// minutes.
pub fn pool_resume_after_unload(env: &ContractEnv) -> CaseResult {
    let c = Check("pool_resume_after_unload");
    let mcp = fixture_mcp();
    let client = c.setup(env.launch(Some(&mcp), None), "starting the app-server")?;
    let mut stream = env.stream(&client);
    let mut session = c.ok(
        env.open(&client, Open::new().mcp(mcp.clone())),
        "opening a thread",
    )?;
    let thread = session.thread_id.clone();
    let mark = marker("p1b", "a");
    env.fake.queue_for(
        &mark,
        [
            Mode::Reply("before".to_string()),
            Mode::Reply("after".to_string()),
        ],
    );
    run_turn(c, &mut stream, &mut session, &format!("{mark} first"))?;
    c.ok(
        session.control().set_goal(
            Some(&format!("{mark} a goal held paused")),
            Some(GoalStatus::Paused),
            None,
        ),
        "setting a paused goal",
    )?;
    c.ok(unsubscribe_thread(&client, &thread), "thread/unsubscribe")?;

    let unsubscribed = Instant::now();
    let unloaded_after = loop {
        let loaded = c.ok(loaded_threads(&client), "thread/loaded/list")?;
        if !loaded.contains(&thread) {
            break Some(unsubscribed.elapsed().as_secs());
        }
        if unsubscribed.elapsed() > Duration::from_secs(40 * 60) {
            break None;
        }
        std::thread::sleep(Duration::from_secs(30));
    };
    let unloaded_after = c.some(unloaded_after, "the thread unloading within 40 minutes")?;

    let set_while_unloaded = client
        .send_request(
            "thread/goal/set",
            json!({ "threadId": thread, "status": GoalStatus::Paused }),
        )
        .map(|_| "accepted".to_string());
    let before_resume = env.fake.served().len();
    let mut resumed = c.ok(
        env.open(&client, Open::new().mcp(mcp).resume(&thread)),
        "resuming the unloaded thread through Cairn's open",
    )?;
    std::thread::sleep(Duration::from_secs(3));
    let after_resume = env.fake.served().len();
    let goal = resumed
        .control()
        .get_goal()
        .map(|goal| goal.map(|goal| format!("{:?}", goal.status)));
    let (_, messages) = run_turn(c, &mut stream, &mut resumed, "second")?;
    let done = messages.last().cloned().unwrap_or_default();

    record(
        "P1b",
        json!({
            "unloadedAfterSecs": unloaded_after,
            "goalSetWhileUnloaded": format!("{set_while_unloaded:?}"),
            "resumedSameThread": resumed.thread_id == thread,
            "requestsStartedByResume": after_resume - before_resume,
            "goalAfterResume": format!("{goal:?}"),
            "resumedTurn": status_name(&done),
        }),
    );
    c.that(resumed.thread_id == thread, || {
        format!(
            "resuming the unloaded {thread} fell back to {}",
            resumed.thread_id
        )
    })?;
    c.that(after_resume == before_resume, || {
        format!(
            "resuming a paused-goal thread started {} request(s) by itself",
            after_resume - before_resume
        )
    })?;
    c.that(
        completed_status(&done) == Some(ProviderTurnStatus::Completed),
        || format!("the resumed thread's turn ended {done}"),
    )?;
    env.no_egress(c)
}

/// How many provider requests carrying `mark` the fake has served. A goal's
/// continuation turns repeat the thread's history, so a marker in its first
/// prompt counts them.
fn served_by(env: &ContractEnv, mark: &str) -> usize {
    env.fake
        .served()
        .iter()
        .filter(|served| served.marker.as_deref() == Some(mark))
        .count()
}

/// P3(b) for recovery: two threads whose goals are active when their server
/// is killed. On the successor one has its goal paused before it is resumed,
/// the way server-loss recovery would; the other, the control, is resumed
/// untouched. The paused thread must start no turn by itself. The control
/// must continue its goal, or the probe could not tell a paused resume from
/// one that ignores the goal altogether.
pub fn pool_active_goal_paused_before_resume_after_server_loss(env: &ContractEnv) -> CaseResult {
    let c = Check("pool_active_goal_paused_before_resume_after_server_loss");
    let mcp = fixture_mcp();
    let client = c.setup(env.launch(Some(&mcp), None), "starting the app-server")?;
    let mut stream = env.stream(&client);
    let marks = [marker("p3r", "paused"), marker("p3r", "control")];
    let mut threads = Vec::new();
    for mark in &marks {
        // Registered with no modes: the fake attributes requests only to a
        // known marker, and answers these normally.
        env.fake.queue_for(mark, []);
        let mut session = c.ok(
            env.open(&client, Open::new().mcp(mcp.clone())),
            "opening a goal thread",
        )?;
        run_turn(c, &mut stream, &mut session, &format!("{mark} start"))?;
        c.ok(
            session.control().set_goal(
                Some("keep working; never mark this goal complete"),
                Some(GoalStatus::Active),
                None,
            ),
            "setting an active goal",
        )?;
        threads.push(session.thread_id.clone());
    }
    let continuing = Instant::now() + TURN;
    while marks.iter().any(|mark| served_by(env, mark) < 3) {
        if Instant::now() > continuing {
            return Err(c.fail(format!(
                "the active goals never continued: {} and {} requests",
                served_by(env, &marks[0]),
                served_by(env, &marks[1])
            )));
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    client.shutdown();
    let server_died = exits_within(&client, Duration::from_secs(10));
    let successor = c.setup(env.launch(Some(&mcp), None), "starting the successor")?;
    let paused_before_resume = successor
        .send_request(
            "thread/goal/set",
            json!({ "threadId": threads[0], "status": GoalStatus::Paused }),
        )
        .map(|_| "accepted".to_string());
    let before: Vec<usize> = marks.iter().map(|mark| served_by(env, mark)).collect();
    let mut paused = c.ok(
        env.open(&successor, Open::new().mcp(mcp.clone()).resume(&threads[0])),
        "resuming the thread whose goal was paused first",
    )?;
    let mut control = c.ok(
        env.open(&successor, Open::new().mcp(mcp).resume(&threads[1])),
        "resuming the control",
    )?;
    std::thread::sleep(Duration::from_secs(3));
    let after: Vec<usize> = marks.iter().map(|mark| served_by(env, mark)).collect();
    let paused_goal = paused
        .control()
        .get_goal()
        .map(|goal| goal.map(|goal| format!("{:?}", goal.status)));
    let _ = control
        .control()
        .set_goal(None, Some(GoalStatus::Paused), None);
    let _ = control
        .control()
        .interrupt(cairn_models::interface::session::InterruptCause::OperatorStop);

    record(
        "P3-recovery",
        json!({
            "serverDied": server_died,
            "pausedBeforeResumeOnSuccessor": format!("{paused_before_resume:?}"),
            "pausedThreadResumedSameThread": paused.thread_id == threads[0],
            "pausedThreadRequestsInThreeSecondsAfterResume": after[0] - before[0],
            "pausedThreadGoalAfterResume": format!("{paused_goal:?}"),
            "controlRequestsInThreeSecondsAfterResume": after[1] - before[1],
        }),
    );
    c.that(paused_before_resume.is_ok(), || {
        format!("the successor refused to pause the unloaded thread's goal before its resume: {paused_before_resume:?}")
    })?;
    c.that(after[1] > before[1], || {
        "the control did not continue its active goal once resumed, so this probe cannot tell a paused resume from one that ignores the goal".to_string()
    })?;
    c.that(after[0] == before[0], || {
        format!(
            "the thread whose goal was paused before its resume started {} request(s) by itself",
            after[0] - before[0]
        )
    })?;
    env.no_egress(c)
}
