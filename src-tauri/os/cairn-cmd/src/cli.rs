//! CLI subcommands (thin client: forward to the running app, print to stdout).
//! `read`/`write`/`watch` build a callback request, forward it over the same
//! HTTP callback the MCP server uses, and print the result.
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use std::env;
use std::time::Duration;

use cairn_common::executor_protocol::{CheckSelectorSource, ExecutorSelector, BATCH_EXECUTOR_ENV};
use cairn_common::protocol::CallbackRequest;
use cairn_mcp_server::output::assemble_reminders;
use cairn_mcp_server::schemas::{ChangeInput, ChangeItemInput, ReadFileInput};
use cairn_mcp_server::CairnMcpServer;

use crate::http_sink::fixed_server;

// ============================================================================
// CLI subcommands (thin client: forward to the running app, print to stdout)
// ============================================================================

pub(crate) fn default_callback_url() -> String {
    // The runner owns the local MCP callback endpoint after the daemon cutover.
    // Agent-spawned processes still receive CAIRN_CALLBACK_URL explicitly, but
    // bare CLI and externally installed MCP invocations should target the runner
    // transport by convention.
    let port = cairn_common::paths::runner_port();
    format!("http://127.0.0.1:{}/api/mcp", port)
}

fn parse_check_run_observation(raw: &str) -> Result<serde_json::Value, String> {
    let value: serde_json::Value = serde_json::from_str(raw)
        .map_err(|error| format!("invalid observation response: {error}"))?;
    if !value
        .get("ok")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        return Err(value
            .get("error")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("manual check observation failed")
            .to_string());
    }

    let observation = value
        .get("observation")
        .ok_or_else(|| "observation response did not contain an observation".to_string())?;
    match observation.get("state").and_then(serde_json::Value::as_str) {
        Some("queued" | "running" | "parked" | "complete" | "failed") => Ok(observation.clone()),
        Some(state) => Err(format!(
            "observation response contained unknown state {state:?}"
        )),
        None => Err("observation response did not contain a state".to_string()),
    }
}

fn check_run_status_request(client: &CairnMcpServer, observation_handle: &str) -> CallbackRequest {
    CallbackRequest {
        thread_id: None,
        cwd: client.cwd().to_string(),
        run_id: client.run_id().map(ToString::to_string),
        tool: "check_run".to_string(),
        payload: serde_json::json!({ "observationHandle": observation_handle }),
        tool_use_id: None,
    }
}

/// Callback URL for CLI use: explicit env var (set by the runner or a remote
/// executor relay for in-run invocations), else the local runner transport port.
pub(crate) fn cli_callback_url() -> String {
    select_callback_url(env::var("CAIRN_CALLBACK_URL").ok())
}

fn select_callback_url(explicit: Option<String>) -> String {
    explicit.unwrap_or_else(default_callback_url)
}

fn select_mcp_secret(
    explicit: Option<String>,
    load_local: impl FnOnce() -> Option<String>,
) -> Option<String> {
    explicit.or_else(load_local)
}

/// Build a thin server client from the environment for CLI forwarding.
pub(crate) fn build_cli_client(callback_url: String) -> CairnMcpServer {
    let cwd = env::current_dir()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    let run_id = env::var("CAIRN_RUN_ID").ok();
    let mcp_secret = select_mcp_secret(
        env::var("CAIRN_MCP_SECRET").ok(),
        cairn_common::auth::load_local_mcp_token,
    );
    fixed_server(callback_url, cwd, run_id, mcp_secret)
}

/// Extract printable text and the error flag from a tool result
/// (version-tolerant: reads the serialized JSON rather than rmcp internals).
fn tool_result_text(result: &CallToolResult) -> (String, bool) {
    let value = serde_json::to_value(result).unwrap_or_default();
    let is_error = value
        .get("isError")
        .or_else(|| value.get("is_error"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let mut out = String::new();
    if let Some(items) = value.get("content").and_then(|c| c.as_array()) {
        for item in items {
            if let Some(t) = item.get("text").and_then(|t| t.as_str()) {
                out.push_str(t);
            }
        }
    }
    (out, is_error)
}

/// Print tool output to the right stream and return success.
fn emit_tool_result(result: &CallToolResult) -> bool {
    let (text, is_error) = tool_result_text(result);
    if is_error {
        if !text.is_empty() {
            eprint!("{text}");
            if !text.ends_with('\n') {
                eprintln!();
            }
        }
    } else {
        print!("{text}");
        if !text.ends_with('\n') {
            println!();
        }
    }
    !is_error
}

/// Parse `host`/`port` from a callback URL.
fn callback_host_port(callback_url: &str) -> Option<(String, u16)> {
    let parsed = url::Url::parse(callback_url).ok()?;
    let host = parsed.host_str()?.to_string();
    let port = parsed.port().unwrap_or(80);
    Some((host, port))
}

/// True if something is listening on the callback endpoint.
fn probe_callback(callback_url: &str) -> bool {
    use std::net::{TcpStream, ToSocketAddrs};
    let Some((host, port)) = callback_host_port(callback_url) else {
        return false;
    };
    (host.as_str(), port)
        .to_socket_addrs()
        .ok()
        .and_then(|mut addrs| addrs.next())
        .map(|addr| TcpStream::connect_timeout(&addr, Duration::from_millis(300)).is_ok())
        .unwrap_or(false)
}

/// Ensure the local MCP callback endpoint is reachable.
pub(crate) async fn ensure_callback_reachable(callback_url: &str) -> bool {
    probe_callback(callback_url)
}

pub(crate) fn print_unreachable_callback(callback_url: &str) {
    eprintln!("{}", unreachable_callback_message(callback_url));
}

pub(crate) fn unreachable_callback_message(callback_url: &str) -> String {
    match operator_stop_explanation(callback_url) {
        Some(refusal) => format!("cairn: {refusal}"),
        None => format!(
            "cairn: requires a running Cairn runner or server at {callback_url} (set CAIRN_CALLBACK_URL to override)."
        ),
    }
}

/// Why a local callback is not answering, when the reason is a person's stop.
///
/// A stopped orchestrator is a state someone chose, not a fault, and the useful
/// answer names it and who can lift it. Only a loopback callback is explained
/// this way: the stop recorded in this machine's Cairn home says nothing about
/// a server elsewhere.
pub(crate) fn operator_stop_explanation(callback_url: &str) -> Option<String> {
    stop_explanation_for(
        callback_url,
        cairn_common::operator_stop::read(&cairn_common::paths::cairn_home()),
        cairn_common::operator_stop::unix_time_ms(),
    )
}

fn stop_explanation_for(
    callback_url: &str,
    stop: Option<cairn_common::operator_stop::OperatorStop>,
    now_unix_ms: u64,
) -> Option<String> {
    let (host, _) = callback_host_port(callback_url)?;
    let loopback = host == "localhost"
        || host
            .trim_matches(['[', ']'])
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback());
    if !loopback {
        return None;
    }
    stop.map(|stop| stop.refusal(now_unix_ms))
}

/// Parse a `ChangeInput` from a JSON object (with `changes`) or a bare array.
fn parse_change_input(raw: &str) -> Result<ChangeInput, String> {
    let value: serde_json::Value = serde_json::from_str(raw).map_err(|e| e.to_string())?;
    if value.is_array() {
        let changes: Vec<ChangeItemInput> =
            serde_json::from_value(value).map_err(|e| e.to_string())?;
        Ok(ChangeInput {
            changes: Some(changes),
            branch: None,
            commit_msg: None,
            preview: None,
            atomic: None,
            conflict_markers_reason: None,
        })
    } else {
        serde_json::from_value(value).map_err(|e| e.to_string())
    }
}

pub(crate) async fn run_cli_read(
    targets: &[String],
    offset: Option<usize>,
    limit: Option<usize>,
) -> bool {
    // --offset/--limit are a single-target human convenience: fold them into the
    // lone target's query string. With multiple targets they are ambiguous, so
    // scope must be expressed per-URI in each target's query.
    let paths = match fold_cli_scope(targets, offset, limit) {
        Ok(paths) => paths,
        Err(message) => {
            eprintln!("cairn read: {message}");
            return false;
        }
    };

    let callback_url = cli_callback_url();
    if !ensure_callback_reachable(&callback_url).await {
        print_unreachable_callback(&callback_url);
        return false;
    }
    let client = build_cli_client(callback_url);
    let input = ReadFileInput { paths };
    match client.read_cli(input).await {
        Ok(result) => emit_tool_result(&result),
        Err(e) => {
            eprintln!("cairn read failed: {e}");
            false
        }
    }
}

/// Fold CLI `--offset`/`--limit` flags into a single target's query string.
/// Errors if either flag is given alongside multiple targets.
fn fold_cli_scope(
    targets: &[String],
    offset: Option<usize>,
    limit: Option<usize>,
) -> Result<Vec<String>, String> {
    if offset.is_none() && limit.is_none() {
        return Ok(targets.to_vec());
    }
    if targets.len() != 1 {
        return Err(
            "--offset/--limit require exactly one target; scope each target via its own query string (e.g. 'file:x.rs?offset=10&limit=20')".to_string(),
        );
    }
    let mut extra: Vec<String> = Vec::new();
    if let Some(offset) = offset {
        extra.push(format!("offset={offset}"));
    }
    if let Some(limit) = limit {
        extra.push(format!("limit={limit}"));
    }
    let target = &targets[0];
    let separator = if target.contains('?') { '&' } else { '?' };
    Ok(vec![format!("{target}{separator}{}", extra.join("&"))])
}

/// Block until an issue needs attention or is done, re-issuing the long-poll
/// transparently.
///
/// Each iteration is a real server-side broadcast await (no client polling); on
/// a `pending` sentinel the loop re-calls carrying the latest `updated_at` as
/// `--since`, so a change that lands between chunks is caught by the next call's
/// current-state check. Returns when the issue is `actionable` (needs the
/// driver) or `resolved` (reached a terminal status — merged/closed/failed).
pub(crate) async fn run_cli_watch(issue_uri: String, since: Option<i64>) -> bool {
    let callback_url = cli_callback_url();
    if !ensure_callback_reachable(&callback_url).await {
        print_unreachable_callback(&callback_url);
        return false;
    }
    let client = build_cli_client(callback_url);
    let mut cursor = since;
    loop {
        let payload = match cursor {
            Some(s) => serde_json::json!({ "issue_uri": issue_uri, "since": s }),
            None => serde_json::json!({ "issue_uri": issue_uri }),
        };
        let request = CallbackRequest {
            thread_id: None,
            cwd: client.cwd().to_string(),
            run_id: client.run_id().map(ToString::to_string),
            tool: "watch".to_string(),
            payload,
            tool_use_id: None,
        };
        let outcome = client.callback(&request).await;
        if !outcome.transport_ok {
            eprintln!("cairn watch: {}", outcome.result.trim_end());
            return false;
        }
        let value: serde_json::Value = match serde_json::from_str(&outcome.result) {
            Ok(v) => v,
            Err(_) => {
                eprintln!("cairn watch: {}", outcome.result.trim_end());
                return false;
            }
        };
        // Augmentation reminders (e.g. a queued DM to the watcher's run) ride as
        // data alongside the JSON event; surface them so they are not lost.
        if !outcome.reminders.is_empty() {
            eprintln!(
                "cairn watch:{}",
                assemble_reminders(String::new(), &outcome.reminders)
            );
        }
        match value.get("status").and_then(|s| s.as_str()) {
            // The issue needs the driver (a question, gated artifact, review),
            // or it reached a terminal status (merged/closed/failed). Either
            // ends the loop: print the typed event JSON the server returned.
            // The `fact` block carries the inline content (question text,
            // permission tool, artifact summary, PR state) so the caller does
            // not have to do a follow-up `read` to act.
            Some("actionable") | Some("resolved") => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&value).unwrap_or(outcome.result)
                );
                return true;
            }
            Some("pending") => {
                // Advance the cursor and re-issue; the server call blocks, so
                // there is no client-side sleep or poll here.
                cursor = value.get("updated_at").and_then(|u| u.as_i64()).or(cursor);
                continue;
            }
            _ => {
                eprintln!("cairn watch: {}", outcome.result.trim_end());
                return false;
            }
        }
    }
}

/// Where a check lane was asked to run, and who asked.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CheckPlacement {
    selector: ExecutorSelector,
    source: CheckSelectorSource,
}

/// The placement this invocation requests, in precedence order: an explicit
/// `--os`/`--executor`, then the pin of the batch whose shell this is
/// (`CAIRN_BATCH_EXECUTOR`), then none. An inherited value that does not parse
/// as a placement pin is ignored rather than refused: the shell did not write
/// it, and the check can still run where its configuration says.
fn check_placement(
    os: Option<String>,
    executor: Option<String>,
    inherited: Option<String>,
) -> Option<CheckPlacement> {
    if os.is_some() || executor.is_some() {
        return Some(CheckPlacement {
            selector: ExecutorSelector {
                name: executor,
                os,
                ..ExecutorSelector::default()
            },
            source: CheckSelectorSource::Explicit,
        });
    }
    let selector = serde_json::from_str::<ExecutorSelector>(inherited?.trim()).ok()?;
    let selector = selector.placement_pin()?;
    selector.validate_placement_pin().ok()?;
    Some(CheckPlacement {
        selector,
        source: CheckSelectorSource::Inherited,
    })
}

fn check_run_request(
    client: &CairnMcpServer,
    suite: String,
    branch: Option<String>,
    retry: bool,
    placement: Option<&CheckPlacement>,
) -> CallbackRequest {
    let mut payload = serde_json::json!({ "suite": suite });
    if let Some(branch) = branch {
        payload["branch"] = serde_json::Value::String(branch);
    }
    if retry {
        payload["retry"] = serde_json::Value::Bool(true);
    }
    if let Some(placement) = placement {
        payload["executor"] =
            serde_json::to_value(&placement.selector).expect("an executor selector serializes");
        payload["selectorSource"] =
            serde_json::to_value(placement.source).expect("a selector source serializes");
    }
    CallbackRequest {
        thread_id: None,
        cwd: client.cwd().to_string(),
        run_id: client.run_id().map(ToString::to_string),
        tool: "check_run".to_string(),
        payload,
        tool_use_id: None,
    }
}

/// The machine a verdict came from, as the result line names it: `fedora
/// (linux)` for a run, `reused from fedora (linux)` for a cached verdict.
fn check_machine_clause(result: &serde_json::Value, disposition: &str) -> Option<String> {
    let executor = result.get("executor").filter(|value| !value.is_null())?;
    let name = executor.get("name").and_then(serde_json::Value::as_str)?;
    let machine = match executor.get("os").and_then(serde_json::Value::as_str) {
        Some(os) => format!("{name} ({os})"),
        None => name.to_string(),
    };
    Some(if disposition == "cached" {
        format!("reused from {machine}")
    } else {
        machine
    })
}

/// What the runner left out of a check result, as something the caller can act on.
fn incomplete_check_result(field: &str) -> String {
    format!("Cairn's runner left {field} out of its check result. Run the command again.")
}

/// Render one manual configured-check reply.
///
/// Three different things come back here and each has to read as itself: a
/// verdict about the tree, a run that produced NO verdict because Cairn's own
/// machinery failed, and a verdict Cairn holds but will not reuse. Returns the
/// text to print and whether the command succeeded.
fn render_check_run_response(raw: &str) -> Result<(String, bool), String> {
    let envelope: serde_json::Value = serde_json::from_str(raw)
        .map_err(|_| format!("invalid response from runner: {}", raw.trim_end()))?;
    if !envelope
        .get("ok")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        return Err(envelope
            .get("error")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("configured check failed")
            .to_string());
    }
    let result = envelope
        .get("result")
        .ok_or_else(|| incomplete_check_result("the check result"))?;
    let suite = result
        .get("checkName")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| incomplete_check_result("the check name"))?;
    let commit = result
        .get("commitSha")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| incomplete_check_result("the commit"))?;
    let short_commit = commit.get(..12).unwrap_or(commit);
    let disposition = result
        .get("disposition")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| incomplete_check_result("the disposition"))?;
    let cache_line = if disposition == "cached" {
        "cache hit: suite input tree, verdict environment, schema, and reusable verdict matched"
    } else {
        "cache miss: no reusable verdict matched suite input tree, verdict environment, and schema"
    };
    // An inherited batch pin the suite's configuration overruled is stated
    // before anything else, so a lane that landed elsewhere says why.
    let cache = match result
        .get("placementNote")
        .and_then(serde_json::Value::as_str)
    {
        Some(note) => format!("{note}\n{cache_line}"),
        None => cache_line.to_string(),
    };
    let machine = check_machine_clause(result, disposition)
        .map(|machine| format!(" · {machine}"))
        .unwrap_or_default();

    // A verdict from a placement the request did not demand is not its
    // answer, whatever it says about the tree, so it never renders as one.
    if let Some(unmet) = result
        .get("placementUnmet")
        .and_then(serde_json::Value::as_str)
    {
        let observation = result
            .get("observationRef")
            .and_then(serde_json::Value::as_str)
            .map(|reference| format!(" · {reference}"))
            .unwrap_or_default();
        return Ok((
            format!(
                "{cache}\n⚠ {suite} has no verdict for the placement you asked for on \
                 {short_commit} ({disposition}){machine}{observation} — {unmet}. Run the \
                 command again."
            ),
            false,
        ));
    }

    // A run that produced no verdict is a fact about Cairn, never a red against
    // the tree, so it renders from its own named cause instead of a ✗ line.
    if let Some(no_verdict) = result.get("noVerdict").filter(|value| !value.is_null()) {
        let cause = no_verdict
            .get("cause")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("Cairn recorded no cause for this failure.");
        let headline = match no_verdict.get("kind").and_then(serde_json::Value::as_str) {
            Some("suppressed") => {
                let after = no_verdict
                    .get("afterFailures")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or_default();
                format!(
                    "⚠ Cairn did not run {suite} at {short_commit}. Cairn stops a check after \
                     {after} infrastructure failures in a row. Ask an operator to read the cause \
                     below."
                )
            }
            _ => format!(
                "⚠ Cairn ran {suite} at {short_commit} and got no verdict. Cairn's own machinery \
                 failed, so this says nothing about your change. Run the command again."
            ),
        };
        return Ok((format!("{cache}\n{headline}\n{cause}"), false));
    }

    let passed = result
        .get("passed")
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| incomplete_check_result("the verdict"))?;
    let not_recorded = result
        .get("notRecorded")
        .and_then(serde_json::Value::as_str);
    if passed
        && !result
            .get("verified")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    {
        let reason = result
            .get("unverifiedReason")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("the runner supplied no coverage evidence");
        return Ok((
            format!(
                "{cache}\n⚠ {suite} is unverified on {short_commit} ({disposition}){machine} — {reason}"
            ),
            false,
        ));
    }
    let observation = match result
        .get("observationRef")
        .and_then(serde_json::Value::as_str)
    {
        Some(reference) => {
            let ran_at = result
                .get("ranAt")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("time unavailable");
            format!("{suite}@{short_commit} · {ran_at} · {reference}")
        }
        None => {
            let cause = not_recorded.ok_or_else(|| {
                incomplete_check_result("the observation recording failure cause")
            })?;
            format!("Cairn recorded no observation for this run: {cause}")
        }
    };
    // Recording failures are supplied by the producer that observed them. An
    // empty fingerprint is not evidence of any particular failure mode. A cause
    // already rendered beside a missing citation must not be repeated as a suffix.
    let reuse = match (
        not_recorded,
        result
            .get("observationRef")
            .and_then(serde_json::Value::as_str),
    ) {
        (Some(reason), Some(_)) => format!(" · {reason}"),
        (Some(_), None) => String::new(),
        (None, _)
            if passed
                && !result
                    .get("reusable")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false) =>
        {
            " · Cairn will not reuse this verdict".to_string()
        }
        (None, _) => String::new(),
    };
    Ok((
        format!(
            "{cache}\n{suite} {} on {short_commit} ({disposition}){machine} · {observation}{reuse}",
            if passed { "✓" } else { "✗" }
        ),
        passed,
    ))
}

pub(crate) async fn run_cli_check(
    suites: Vec<String>,
    branch: Option<String>,
    retry: bool,
    os: Option<String>,
    executor: Option<String>,
) -> i32 {
    let placement = check_placement(os, executor, env::var(BATCH_EXECUTOR_ENV).ok());
    let callback_url = cli_callback_url();
    if !ensure_callback_reachable(&callback_url).await {
        print_unreachable_callback(&callback_url);
        return 1;
    }
    let client = build_cli_client(callback_url);
    let requests = suites
        .iter()
        .cloned()
        .map(|suite| check_run_request(&client, suite, branch.clone(), retry, placement.as_ref()));
    let outcomes = futures::future::join_all(requests.map(|request| {
        let client = &client;
        async move { client.callback(&request).await }
    }))
    .await;
    let mut all_passed = true;
    let mut started = Vec::new();
    for (suite, outcome) in suites.into_iter().zip(outcomes) {
        if !outcome.transport_ok {
            eprintln!("cairn check run: {}", outcome.result.trim_end());
            all_passed = false;
            continue;
        }
        let start: serde_json::Value = match serde_json::from_str(&outcome.result) {
            Ok(value) => value,
            Err(error) => {
                eprintln!("cairn check run: invalid start response: {error}");
                all_passed = false;
                continue;
            }
        };
        if let Some(error) = start
            .get("ok")
            .and_then(serde_json::Value::as_bool)
            .filter(|ok| !ok)
            .and_then(|_| start.get("error"))
            .and_then(serde_json::Value::as_str)
        {
            eprintln!("cairn check run: {error}");
            all_passed = false;
            continue;
        }
        let Some(handle) = start
            .get("observationHandle")
            .and_then(serde_json::Value::as_str)
        else {
            eprintln!("cairn check run: start response did not contain an observation handle");
            all_passed = false;
            continue;
        };
        eprintln!(
            "cairn check run: {}",
            check_run_start_line(&suite, handle, retry)
        );
        started.push((suite, handle.to_string()));
    }
    let stop = stop_requested();
    tokio::pin!(stop);
    for (position, (suite, handle)) in started.iter().enumerate() {
        let last = std::sync::Mutex::new(None);
        let final_response = tokio::select! {
            settled = watch_check_run(&client, suite, handle, &last) => settled,
            exit_code = &mut stop => {
                let now = unix_now_ms();
                let last = last.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                eprintln!(
                    "cairn check run: {}",
                    check_run_stop_line(last.as_ref(), suite, handle, now)
                );
                for (suite, handle) in &started[position + 1..] {
                    eprintln!(
                        "cairn check run: {}",
                        check_run_stop_line(None, suite, handle, now)
                    );
                }
                // The signal's conventional code, never 1: an abandoned wait
                // is not a failed check.
                return exit_code;
            }
        };
        let Some(final_response) = final_response else {
            all_passed = false;
            continue;
        };
        match render_check_run_response(&final_response) {
            Ok((summary, passed)) => {
                println!("{summary}");
                all_passed &= passed;
            }
            Err(error) => {
                eprintln!("cairn check run: {error}");
                all_passed = false;
            }
        }
    }
    if all_passed {
        0
    } else {
        1
    }
}

/// How often a waiting `cairn check run` restates where its check stands. A
/// requester can be killed without warning (a run item's timeout, SIGKILL), so
/// this bounds how stale the last line it printed can be.
const CHECK_RUN_HEARTBEAT: Duration = Duration::from_secs(10 * 60);

/// Where one manual check stands, as its requester last observed it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CheckWatch {
    state: String,
    diagnostic: String,
    executor: Option<String>,
    since_unix_ms: Option<u64>,
}

impl CheckWatch {
    /// `None` for a settled observation.
    fn from_observation(observation: &serde_json::Value) -> Option<Self> {
        let state = observation.get("state")?.as_str()?;
        if !matches!(state, "queued" | "running" | "parked") {
            return None;
        }
        let text = |key: &str| {
            observation
                .get(key)
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        };
        Some(Self {
            state: state.to_string(),
            diagnostic: text("diagnostic").unwrap_or_else(|| state.to_string()),
            executor: text("executor"),
            since_unix_ms: observation
                .get("sinceUnixMs")
                .and_then(serde_json::Value::as_u64),
        })
    }

    /// The state, the machine, or the start time moved since `previous`.
    fn is_news_after(&self, previous: Option<&Self>) -> bool {
        previous.is_none_or(|previous| {
            previous.state != self.state
                || previous.executor != self.executor
                || previous.since_unix_ms != self.since_unix_ms
        })
    }

    /// Where the check stands, for a line that may be the last one printed:
    /// `RUNNING on fedora since 09:48:09Z (1h10m elapsed)`.
    fn standing(&self, now_unix_ms: u64) -> String {
        let state = self.state.to_uppercase();
        match (&self.executor, self.since_unix_ms) {
            (Some(executor), Some(since)) if self.state == "running" => format!(
                "{state} on {executor} since {} ({} elapsed)",
                clock_utc(since),
                describe_elapsed(now_unix_ms.saturating_sub(since)),
            ),
            (_, Some(since)) => format!(
                "{state} since {} ({} elapsed): {}",
                clock_utc(since),
                describe_elapsed(now_unix_ms.saturating_sub(since)),
                self.diagnostic,
            ),
            (_, None) => format!("{state}: {}", self.diagnostic),
        }
    }

    fn heartbeat_line(&self, suite: &str, now_unix_ms: u64) -> String {
        format!("{suite} is still {}", self.standing(now_unix_ms))
    }
}

fn check_run_start_line(suite: &str, handle: &str, retry: bool) -> String {
    // Both halves are what the runner does: an identical request coalesces onto
    // the execution in flight whatever `--retry` says, and without `--retry` a
    // reusable recorded row answers before anything is placed.
    let rejoin = if retry {
        "re-running with --retry joins it while it runs, and runs it afresh once its row is recorded"
    } else {
        "re-running the same command joins it while it runs, and reuses its verdict once recorded where that verdict is reusable"
    };
    format!(
        "{suite} observation {handle}; the check runs to its recorded row even if this command is stopped; {rejoin}"
    )
}

fn check_run_stop_line(
    last: Option<&CheckWatch>,
    suite: &str,
    handle: &str,
    now_unix_ms: u64,
) -> String {
    let standing = last.map_or_else(
        || "was started".to_string(),
        |watch| format!("is {}", watch.standing(now_unix_ms)),
    );
    format!(
        "stopped waiting; {suite} {standing}; its row will still be recorded (observation {handle})"
    )
}

/// Poll one observation until it settles: the result to render, or `None`
/// once a failure has been reported. `last` always holds the latest unsettled
/// observation, for the line printed if the wait is abandoned.
async fn watch_check_run(
    client: &CairnMcpServer,
    suite: &str,
    handle: &str,
    last: &std::sync::Mutex<Option<CheckWatch>>,
) -> Option<String> {
    let mut last_printed = tokio::time::Instant::now();
    loop {
        let status = client
            .callback(&check_run_status_request(client, handle))
            .await;
        if !status.transport_ok {
            eprintln!("cairn check run: {}", status.result.trim_end());
            return None;
        }
        let observation = match parse_check_run_observation(&status.result) {
            Ok(observation) => observation,
            Err(error) => {
                eprintln!("cairn check run: {error}");
                return None;
            }
        };
        if let Some(watch) = CheckWatch::from_observation(&observation) {
            let mut last = last
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if watch.is_news_after(last.as_ref()) {
                eprintln!("cairn check run: {}", watch.diagnostic);
                last_printed = tokio::time::Instant::now();
            } else if last_printed.elapsed() >= CHECK_RUN_HEARTBEAT {
                eprintln!(
                    "cairn check run: {}",
                    watch.heartbeat_line(suite, unix_now_ms())
                );
                last_printed = tokio::time::Instant::now();
            }
            *last = Some(watch);
        }
        match observation.get("state").and_then(serde_json::Value::as_str) {
            Some("complete") => {
                return Some(
                    serde_json::json!({
                        "ok": true,
                        "result": observation.get("result").cloned().unwrap_or_default()
                    })
                    .to_string(),
                )
            }
            Some("failed") => {
                eprintln!(
                    "cairn check run: {}",
                    observation
                        .get("error")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("manual check failed")
                );
                return None;
            }
            _ => tokio::time::sleep(Duration::from_secs(1)).await,
        }
    }
}

/// Resolves when this process is asked to stop, with the exit code a process
/// ended by that signal reports: 130 for Ctrl-C, 143 for SIGTERM. SIGKILL
/// cannot be caught, which is why [`CHECK_RUN_HEARTBEAT`] exists. A signal that
/// cannot be registered never resolves, rather than reading as a request to
/// stop.
async fn stop_requested() -> i32 {
    const INTERRUPTED: i32 = 130;
    async fn interrupted() {
        if tokio::signal::ctrl_c().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
    #[cfg(unix)]
    {
        const TERMINATED: i32 = 143;
        use tokio::signal::unix::{signal, SignalKind};
        if let Ok(mut terminate) = signal(SignalKind::terminate()) {
            return tokio::select! {
                () = interrupted() => INTERRUPTED,
                _ = terminate.recv() => TERMINATED,
            };
        }
    }
    interrupted().await;
    INTERRUPTED
}

fn unix_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

/// `09:48:09Z`, the form the runner's own check diagnostics use.
fn clock_utc(unix_ms: u64) -> String {
    let seconds = unix_ms / 1_000 % 86_400;
    format!(
        "{:02}:{:02}:{:02}Z",
        seconds / 3_600,
        seconds % 3_600 / 60,
        seconds % 60
    )
}

fn describe_elapsed(elapsed_ms: u64) -> String {
    let seconds = elapsed_ms / 1_000;
    match (seconds / 3_600, seconds % 3_600 / 60) {
        (0, 0) => format!("{seconds}s"),
        (0, minutes) => format!("{minutes}m"),
        (hours, minutes) => format!("{hours}h{minutes}m"),
    }
}

pub(crate) async fn run_cli_change(json: Option<String>, commit_msg: Option<String>) -> bool {
    let raw = match json {
        Some(j) => j,
        None => {
            use std::io::Read;
            let mut buf = String::new();
            if std::io::stdin().read_to_string(&mut buf).is_err() {
                eprintln!("cairn write: failed to read JSON from stdin");
                return false;
            }
            buf
        }
    };
    let mut input = match parse_change_input(&raw) {
        Ok(input) => input,
        Err(e) => {
            eprintln!("cairn write: invalid JSON: {e}");
            return false;
        }
    };
    if commit_msg.is_some() {
        input.commit_msg = commit_msg;
    }
    let callback_url = cli_callback_url();
    if !ensure_callback_reachable(&callback_url).await {
        print_unreachable_callback(&callback_url);
        return false;
    }
    let client = build_cli_client(callback_url);
    match client.write_cli(Parameters(input)).await {
        Ok(result) => emit_tool_result(&result),
        Err(e) => {
            eprintln!("cairn write failed: {e}");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// While a person's stop stands, an agent or CLI call to the local runner
    /// learns that state and who can lift it, not a connection error. A remote
    /// callback is never explained by this machine's stop.
    #[test]
    fn a_local_runner_stopped_by_a_person_is_named_as_stopped() {
        let stop = cairn_common::operator_stop::OperatorStop {
            at_unix_ms: 1_000,
            via: "the Cairn app's quit dialog".to_string(),
        };
        for local in [
            "http://127.0.0.1:3849/api/mcp",
            "http://localhost:3849/api/mcp",
            "http://[::1]:3849/api/mcp",
        ] {
            let refusal = stop_explanation_for(local, Some(stop.clone()), 61_000)
                .unwrap_or_else(|| panic!("{local} is the local runner"));
            assert!(refusal.contains("is stopped"), "{refusal}");
            assert!(refusal.contains("cairn-orch start"), "{refusal}");
        }
        assert_eq!(
            stop_explanation_for("https://runner.example.com/api/mcp", Some(stop), 61_000),
            None
        );
        assert_eq!(
            stop_explanation_for("http://127.0.0.1:3849/api/mcp", None, 61_000),
            None
        );
    }

    #[test]
    fn explicit_relay_callback_and_capability_win_unchanged() {
        let callback = "http://127.0.0.1:49152/api/mcp".to_string();
        let capability = "batch-callback-capability".to_string();

        assert_eq!(select_callback_url(Some(callback.clone())), callback);
        assert_eq!(
            select_mcp_secret(Some(capability.clone()), || {
                panic!("an explicit batch capability must not read the local runner secret")
            }),
            Some(capability)
        );
    }

    #[test]
    fn unconfigured_shell_retains_local_runner_fallbacks() {
        assert_eq!(select_callback_url(None), default_callback_url());
        assert_eq!(
            select_mcp_secret(None, || Some("local-runner-secret".into())),
            Some("local-runner-secret".into())
        );
    }

    #[test]
    fn fold_cli_scope_folds_single_target() {
        let paths = fold_cli_scope(&["file:x.rs".to_string()], Some(10), Some(20)).unwrap();
        assert_eq!(paths, vec!["file:x.rs?offset=10&limit=20".to_string()]);
    }

    #[test]
    fn fold_cli_scope_appends_with_ampersand_when_query_present() {
        let paths = fold_cli_scope(&["file:x.rs?grep=foo".to_string()], Some(10), None).unwrap();
        assert_eq!(paths, vec!["file:x.rs?grep=foo&offset=10".to_string()]);
    }

    #[test]
    fn fold_cli_scope_passes_targets_through_without_flags() {
        let targets = vec!["file:a.rs".to_string(), "file:b.rs".to_string()];
        let paths = fold_cli_scope(&targets, None, None).unwrap();
        assert_eq!(paths, targets);
    }

    #[test]
    fn fold_cli_scope_rejects_flags_with_multiple_targets() {
        let targets = vec!["file:a.rs".to_string(), "file:b.rs".to_string()];
        let err = fold_cli_scope(&targets, Some(10), None).unwrap_err();
        assert!(err.contains("exactly one target"));
    }

    #[test]
    fn check_run_request_omits_default_branch() {
        let client = fixed_server(
            "http://localhost".into(),
            "/repo".into(),
            Some("run-1".into()),
            None,
        );
        let request = check_run_request(&client, "rust-tests".into(), None, false, None);
        assert_eq!(request.tool, "check_run");
        assert_eq!(
            request.payload,
            serde_json::json!({ "suite": "rust-tests" })
        );
        assert_eq!(request.run_id.as_deref(), Some("run-1"));
    }

    #[test]
    fn check_run_request_carries_explicit_retry() {
        let client = fixed_server(
            "http://localhost".into(),
            "/repo".into(),
            Some("run-1".into()),
            None,
        );
        let request = check_run_request(&client, "rust-tests".into(), None, true, None);
        assert_eq!(
            request.payload,
            serde_json::json!({ "suite": "rust-tests", "retry": true })
        );
    }

    #[test]
    fn check_placement_prefers_an_explicit_flag_then_the_batch_pin() {
        let pinned = Some(r#"{"os":"linux"}"#.to_string());
        let explicit = check_placement(None, Some("bgsrv".into()), pinned.clone()).unwrap();
        assert_eq!(explicit.source, CheckSelectorSource::Explicit);
        assert_eq!(explicit.selector.name.as_deref(), Some("bgsrv"));
        assert_eq!(explicit.selector.os, None);

        let inherited = check_placement(None, None, pinned).unwrap();
        assert_eq!(inherited.source, CheckSelectorSource::Inherited);
        assert_eq!(inherited.selector.os.as_deref(), Some("linux"));

        assert_eq!(check_placement(None, None, None), None);
        assert_eq!(
            check_placement(None, None, Some("not json".into())),
            None,
            "an unparseable inherited pin is ignored, never refused"
        );
        assert_eq!(
            check_placement(
                None,
                None,
                Some(r#"{"requiredToolchains":["rust"]}"#.into())
            ),
            None,
            "a pin with no name or platform carries nothing to inherit"
        );
    }

    #[test]
    fn check_run_request_carries_the_selector_and_its_source() {
        let client = fixed_server(
            "http://localhost".into(),
            "/repo".into(),
            Some("run-1".into()),
            None,
        );
        let placement = check_placement(Some("linux".into()), None, None).unwrap();
        let request =
            check_run_request(&client, "rust-tests".into(), None, false, Some(&placement));
        assert_eq!(
            request.payload,
            serde_json::json!({
                "suite": "rust-tests",
                "executor": { "os": "linux" },
                "selectorSource": "explicit"
            })
        );
        let inherited = check_placement(None, None, Some(r#"{"name":"fedora"}"#.into())).unwrap();
        let request =
            check_run_request(&client, "rust-tests".into(), None, false, Some(&inherited));
        assert_eq!(request.payload["selectorSource"], "inherited");
        assert_eq!(
            request.payload["executor"],
            serde_json::json!({ "name": "fedora" })
        );
    }

    #[test]
    fn check_run_status_request_carries_only_the_observation_handle() {
        let client = fixed_server(
            "http://localhost".into(),
            "/repo".into(),
            Some("run-1".into()),
            None,
        );
        let request = check_run_status_request(&client, "manual-obs-1");
        assert_eq!(request.tool, "check_run");
        assert_eq!(
            request.payload,
            serde_json::json!({ "observationHandle": "manual-obs-1" })
        );
    }

    #[test]
    fn check_run_unknown_handle_is_terminal() {
        let error = parse_check_run_observation(
            r#"{"ok":false,"error":"manual check observation missing is unknown or expired"}"#,
        )
        .unwrap_err();
        assert!(error.contains("unknown or expired"));
    }

    /// 2026-10-19T09:48:09Z.
    const CLAIMED_AT: u64 = 1_792_403_289_000;

    fn running_on_fedora() -> CheckWatch {
        CheckWatch::from_observation(&serde_json::json!({
            "state": "running",
            "diagnostic": "running on fedora since 09:48:09Z",
            "executor": "fedora",
            "sinceUnixMs": CLAIMED_AT,
        }))
        .unwrap()
    }

    #[test]
    fn check_run_start_line_names_the_handle_and_what_rerunning_does() {
        let line = check_run_start_line("rust-tests", "obs-1", false);
        assert_eq!(
            line,
            "rust-tests observation obs-1; the check runs to its recorded row even if this command is stopped; re-running the same command joins it while it runs, and reuses its verdict once recorded where that verdict is reusable"
        );
        assert!(check_run_start_line("rust-tests", "obs-1", true)
            .contains("runs it afresh once its row is recorded"));
    }

    #[test]
    fn check_run_news_is_a_move_of_state_machine_or_since() {
        let running = running_on_fedora();
        assert!(running.is_news_after(None));
        assert!(!running.is_news_after(Some(&running)));
        let mut parked = running.clone();
        parked.state = "parked".into();
        parked.executor = None;
        assert!(running.is_news_after(Some(&parked)));
        let mut reworded = running.clone();
        reworded.diagnostic = "another wording".into();
        assert!(
            !running.is_news_after(Some(&reworded)),
            "a reworded diagnostic waits for the heartbeat"
        );
    }

    #[test]
    fn check_run_heartbeat_counts_from_the_claim() {
        let line = running_on_fedora().heartbeat_line("rust-tests", CLAIMED_AT + 70 * 60_000);
        assert_eq!(
            line,
            "rust-tests is still RUNNING on fedora since 09:48:09Z (1h10m elapsed)"
        );
        let parked = CheckWatch::from_observation(&serde_json::json!({
            "state": "parked",
            "diagnostic": "parked since 09:48:09Z; wait unpredictable (forecast 4m52s at 09:48:09Z, waited 10m)",
            "sinceUnixMs": CLAIMED_AT,
        }))
        .unwrap();
        assert_eq!(
            parked.heartbeat_line("rust-tests", CLAIMED_AT + 12 * 60_000),
            "rust-tests is still PARKED since 09:48:09Z (12m elapsed): parked since 09:48:09Z; wait unpredictable (forecast 4m52s at 09:48:09Z, waited 10m)"
        );
    }

    /// The line a requester leaves behind says the check goes on, and never
    /// that it timed out.
    #[test]
    fn check_run_stop_line_says_the_check_goes_on() {
        let line = check_run_stop_line(
            Some(&running_on_fedora()),
            "rust-tests",
            "obs-1",
            CLAIMED_AT + 3 * 3_600_000 + 5 * 60_000,
        );
        assert_eq!(
            line,
            "stopped waiting; rust-tests is RUNNING on fedora since 09:48:09Z (3h5m elapsed); its row will still be recorded (observation obs-1)"
        );
        assert!(!line.contains("timed out"));
        assert_eq!(
            check_run_stop_line(None, "lint", "obs-2", CLAIMED_AT),
            "stopped waiting; lint was started; its row will still be recorded (observation obs-2)"
        );
    }

    /// A runner that predates the machine and the start time still renders.
    #[test]
    fn check_run_observation_without_since_still_reads() {
        let watch = CheckWatch::from_observation(&serde_json::json!({
            "state": "running",
            "diagnostic": "running configured check",
        }))
        .unwrap();
        assert_eq!(
            watch.standing(CLAIMED_AT),
            "RUNNING: running configured check"
        );
        assert!(
            CheckWatch::from_observation(&serde_json::json!({ "state": "complete" })).is_none()
        );
    }

    #[test]
    fn check_run_unknown_state_is_terminal() {
        let error = parse_check_run_observation(r#"{"ok":true,"observation":{"state":"mystery"}}"#)
            .unwrap_err();
        assert!(error.contains("unknown state"));
    }

    #[test]
    fn check_run_response_renders_hit_and_its_observation() {
        let rendered = render_check_run_response(
            &serde_json::json!({
                "ok": true,
                "result": {
                    "checkName": "rust-tests",
                    "commitSha": "1234567890abcdef",
                    "disposition": "cached",
                    "passed": true,
                    "verified": true,
                    "reusable": true,
                    "environmentFingerprint": "env-local",
                    "observationRef": "cairn://p/CAIRN/check-observations/EjRWeJCrze8SNFZ4kKvN7w",
                    "ranAt": "2026-08-03 11:12 PDT"
                }
            })
            .to_string(),
        )
        .unwrap();
        assert_eq!(
            rendered,
            (
                "cache hit: suite input tree, verdict environment, schema, and reusable verdict matched\nrust-tests ✓ on 1234567890ab (cached) · rust-tests@1234567890ab · 2026-08-03 11:12 PDT · cairn://p/CAIRN/check-observations/EjRWeJCrze8SNFZ4kKvN7w".to_string(),
                true,
            )
        );
    }

    /// A run names the machine it ran on, a reused verdict names its source's,
    /// and a batch pin the suite overruled says so first.
    #[test]
    fn check_run_response_names_the_machine_and_an_unapplied_pin() {
        let render = |disposition: &str, note: Option<&str>| {
            render_check_run_response(
                &serde_json::json!({
                    "ok": true,
                    "result": {
                        "checkName": "rust-tests",
                        "commitSha": "abc1234567890",
                        "disposition": disposition,
                        "passed": true,
                        "verified": true,
                        "reusable": true,
                        "observationRef": "cairn://p/CAIRN/check-observations/h",
                        "ranAt": "t",
                        "executor": { "name": "fedora", "os": "linux" },
                        "verdictPlatform": "linux",
                        "placementNote": note,
                    }
                })
                .to_string(),
            )
            .unwrap()
            .0
        };
        let fresh = render("fresh", None);
        assert!(
            fresh.ends_with("rust-tests ✓ on abc123456789 (fresh) · fedora (linux) · rust-tests@abc123456789 · t · cairn://p/CAIRN/check-observations/h"),
            "{fresh}"
        );
        let cached = render("cached", None);
        assert!(
            cached.contains("(cached) · reused from fedora (linux) ·"),
            "{cached}"
        );
        let noted = render("fresh", Some("batch pin os=windows not applied: no"));
        assert!(
            noted.starts_with("batch pin os=windows not applied: no\ncache miss"),
            "{noted}"
        );
    }

    /// A verdict produced for another placement than the one demanded never
    /// reads as this request's fresh answer, and the command does not succeed.
    #[test]
    fn check_run_response_refuses_a_verdict_for_another_placement() {
        let (rendered, ok) = render_check_run_response(
            &serde_json::json!({
                "ok": true,
                "result": {
                    "checkName": "rust-tests",
                    "commitSha": "f69ed3b1ed36aa",
                    "disposition": "unmet",
                    "passed": false,
                    "verified": false,
                    "reusable": false,
                    "observationRef": "cairn://p/cairn/check-observations/h",
                    "executor": { "name": "fedora", "os": "linux" },
                    "verdictPlatform": "linux",
                    "placementUnmet": "this verdict was produced on fedora for linux, but this request counts only verdicts for macos",
                }
            })
            .to_string(),
        )
        .unwrap();
        assert!(!ok);
        assert!(!rendered.contains("(fresh)"), "{rendered}");
        assert!(
            rendered.contains("⚠ rust-tests has no verdict for the placement you asked for on f69ed3b1ed36 (unmet) · fedora (linux) · cairn://p/cairn/check-observations/h — this verdict was produced on fedora for linux, but this request counts only verdicts for macos."),
            "{rendered}"
        );
    }

    #[test]
    fn check_run_response_refuses_to_render_an_empty_selection_as_green() {
        let (summary, passed) = render_check_run_response(
            &serde_json::json!({
                "ok": true,
                "result": {
                    "checkName": "rust-tests",
                    "commitSha": "1234567890abcdef",
                    "disposition": "fresh",
                    "passed": true,
                    "verified": false,
                    "unverifiedReason": "no tests matched the selection",
                    "reusable": false
                }
            })
            .to_string(),
        )
        .unwrap();
        assert!(!passed);
        assert!(summary.contains("⚠ rust-tests is unverified"), "{summary}");
        assert!(!summary.contains("rust-tests ✓"), "{summary}");
    }

    #[test]
    fn check_run_response_preserves_a_failed_verdict() {
        let (summary, passed) = render_check_run_response(
            &serde_json::json!({
                "ok": true,
                "result": {
                    "checkName": "rust-tests",
                    "commitSha": "1234567890abcdef",
                    "disposition": "fresh",
                    "passed": false,
                    "reusable": false,
                    "environmentFingerprint": "env-local",
                    "observationRef": "cairn://p/CAIRN/check-observations/EjRWeJCrze8SNFZ4kKvN7w",
                    "ranAt": "2026-08-03 11:12 PDT"
                }
            })
            .to_string(),
        )
        .unwrap();
        assert_eq!(
            summary,
            "cache miss: no reusable verdict matched suite input tree, verdict environment, and schema\nrust-tests ✗ on 1234567890ab (fresh) · rust-tests@1234567890ab · 2026-08-03 11:12 PDT · cairn://p/CAIRN/check-observations/EjRWeJCrze8SNFZ4kKvN7w"
        );
        assert!(
            !passed,
            "a rendered red verdict must produce a nonzero CLI exit"
        );
    }

    /// The specimen this rendering exists for: a check that ran on another
    /// machine. Its verdict comes back like any other, and the producer carries
    /// the concrete reason it could not become reusable evidence.
    #[test]
    fn check_run_response_returns_a_remotely_executed_verdict() {
        let (summary, passed) = render_check_run_response(
            &serde_json::json!({
                "ok": true,
                "result": {
                    "checkName": "rust-tests",
                    "commitSha": "1234567890abcdef",
                    "disposition": "fresh",
                    "passed": true,
                    "verified": true,
                    "reusable": false,
                    "environmentFingerprint": "",
                    "notRecorded": "the selected legacy executor did not report a complete verdict environment identity",
                    "observationRef": "cairn://p/CAIRN/check-observations/782ri3MzIRCrS7e-3avq_w",
                    "ranAt": "2026-08-03 11:12 PDT"
                }
            })
            .to_string(),
        )
        .unwrap();
        assert!(passed, "a remote green is still a green");
        assert!(summary.contains("rust-tests ✓ on 1234567890ab (fresh) · rust-tests@1234567890ab"));
        assert!(!summary.contains("obs-remote"));
        assert!(
            summary
                .contains("legacy executor did not report a complete verdict environment identity"),
            "an unreusable green must say why: {summary}"
        );
    }

    #[test]
    fn check_run_response_names_the_substrate_cause_of_a_no_verdict_run() {
        let (summary, passed) = render_check_run_response(
            &serde_json::json!({
                "ok": true,
                "result": {
                    "checkName": "rust-tests",
                    "commitSha": "1234567890abcdef",
                    "disposition": "fresh",
                    "passed": false,
                    "reusable": false,
                    "environmentFingerprint": "",
                    "observationId": "obs-infra",
                    "noVerdict": {
                        "kind": "infrastructure",
                        "afterFailures": null,
                        "cause": "Cairn could not reach the machine that was running this check."
                    }
                }
            })
            .to_string(),
        )
        .unwrap();
        assert!(!passed);
        assert!(
            summary.contains("got no verdict") && summary.contains("Run the command again"),
            "an infrastructure failure must render as itself: {summary}"
        );
        assert!(
            summary.contains("could not reach the machine"),
            "the named substrate cause must survive to the reader: {summary}"
        );
        assert!(
            !summary.contains('✗'),
            "a run with no verdict must not render as a red against the tree: {summary}"
        );
    }

    #[test]
    fn check_run_response_reports_a_suppressed_check_as_not_run() {
        let (summary, passed) = render_check_run_response(
            &serde_json::json!({
                "ok": true,
                "result": {
                    "checkName": "rust-tests",
                    "commitSha": "1234567890abcdef",
                    "disposition": "fresh",
                    "passed": false,
                    "reusable": false,
                    "environmentFingerprint": "",
                    "observationId": null,
                    "noVerdict": {
                        "kind": "suppressed",
                        "afterFailures": 3,
                        "cause": "the last failure was a spawn error"
                    }
                }
            })
            .to_string(),
        )
        .unwrap();
        assert!(!passed);
        assert!(
            summary.contains("did not run rust-tests") && summary.contains("3 infrastructure"),
            "a suppressed check must say Cairn declined to run it: {summary}"
        );
    }

    #[test]
    fn check_run_response_states_when_no_observation_was_recorded() {
        let (summary, passed) = render_check_run_response(
            &serde_json::json!({
                "ok": true,
                "result": {
                    "checkName": "rust-tests",
                    "commitSha": "1234567890abcdef",
                    "disposition": "fresh",
                    "passed": true,
                    "verified": true,
                    "reusable": false,
                    "environmentFingerprint": "",
                    "observationId": null,
                    "notRecorded": "the observation write failed: database is read-only"
                }
            })
            .to_string(),
        )
        .unwrap();
        assert!(passed, "an unrecorded verdict is still the verdict");
        assert!(
            summary.contains("recorded no observation"),
            "an unrecorded run must say so instead of failing: {summary}"
        );
        assert!(summary.contains("observation write failed: database is read-only"));
    }

    #[test]
    fn check_run_response_names_a_cited_red_non_reuse_cause() {
        let (summary, passed) = render_check_run_response(
            &serde_json::json!({
                "ok": true,
                "result": {
                    "checkName": "rust-lint",
                    "commitSha": "1234567890abcdef",
                    "disposition": "fresh",
                    "passed": false,
                    "reusable": false,
                    "environmentFingerprint": "",
                    "observationRef": "cairn://p/CAIRN/check-observations/782ri3MzIRCrS7e-3avq_w",
                    "ranAt": "2026-08-03 11:12 PDT",
                    "notRecorded": "the selected legacy executor did not report a complete verdict environment identity"
                }
            })
            .to_string(),
        )
        .unwrap();
        assert!(!passed, "a cited remote red remains a red verdict");
        assert!(summary.contains("cairn://p/CAIRN/check-observations/782ri3MzIRCrS7e-3avq_w"));
        assert!(summary.contains(
            "the selected legacy executor did not report a complete verdict environment identity"
        ));
    }

    #[test]
    fn check_run_response_names_a_red_observation_write_failure() {
        let (summary, passed) = render_check_run_response(
            &serde_json::json!({
                "ok": true,
                "result": {
                    "checkName": "rust-lint",
                    "commitSha": "1234567890abcdef",
                    "disposition": "fresh",
                    "passed": false,
                    "reusable": false,
                    "environmentFingerprint": "env-local",
                    "observationId": null,
                    "notRecorded": "the observation write failed: transaction conflict"
                }
            })
            .to_string(),
        )
        .unwrap();
        assert!(!passed, "an unrecorded red remains a red verdict");
        assert!(summary.contains(
            "Cairn recorded no observation for this run: the observation write failed: transaction conflict"
        ));
    }

    #[test]
    fn check_run_response_refuses_a_bare_missing_observation() {
        let error = render_check_run_response(
            &serde_json::json!({
                "ok": true,
                "result": {
                    "checkName": "rust-lint",
                    "commitSha": "1234567890abcdef",
                    "disposition": "fresh",
                    "passed": false,
                    "reusable": false,
                    "environmentFingerprint": "env-local",
                    "observationId": null
                }
            })
            .to_string(),
        )
        .unwrap_err();
        assert!(error.contains("observation recording failure cause"));
    }

    #[test]
    fn check_run_response_does_not_infer_a_reason_from_an_empty_fingerprint() {
        let (summary, passed) = render_check_run_response(
            &serde_json::json!({
                "ok": true,
                "result": {
                    "checkName": "rust-tests",
                    "commitSha": "1234567890abcdef",
                    "disposition": "fresh",
                    "passed": true,
                    "verified": true,
                    "reusable": false,
                    "environmentFingerprint": "",
                    "observationRef": "cairn://p/CAIRN/check-observations/782ri3MzIRCrS7e-3avq_w"
                }
            })
            .to_string(),
        )
        .unwrap();
        assert!(passed);
        assert!(summary.contains("Cairn will not reuse this verdict"));
        assert!(!summary.contains("another machine"));
    }

    #[test]
    fn check_run_response_propagates_unknown_suite_as_failure() {
        let error = render_check_run_response(
            &serde_json::json!({
                "ok": false,
                "error": "configured check unknown was not found"
            })
            .to_string(),
        )
        .unwrap_err();
        assert!(error.contains("unknown"));
    }

    #[test]
    fn check_run_request_forwards_branch_as_an_opaque_revision() {
        let client = fixed_server("http://localhost".into(), "/repo".into(), None, None);
        let request = check_run_request(
            &client,
            "rust-tests".into(),
            Some("main@origin".into()),
            false,
            None,
        );
        assert_eq!(
            request.payload,
            serde_json::json!({
                "suite": "rust-tests",
                "branch": "main@origin"
            })
        );
    }
}
