//! CLI subcommands (thin client: forward to the running app, print to stdout).
//! `read`/`write`/`watch` build a callback request, forward it over the same
//! HTTP callback the MCP server uses, and print the result.
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use std::env;
use std::time::Duration;

use cairn_common::protocol::CallbackRequest;
use cairn_common::uri::parse_uri as parse_cairn_uri;

use crate::output::assemble_reminders;
use crate::schemas::{ChangeInput, ChangeItemInput, ReadFileInput};
use crate::server::CairnCmd;

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

/// Callback URL for CLI use: explicit env var (set by the runner or a remote
/// executor relay for in-run invocations), else the local runner transport port.
fn cli_callback_url() -> String {
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

/// Build a thin `CairnCmd` client from the environment for CLI forwarding.
fn build_cli_client(callback_url: String) -> CairnCmd {
    let cwd = env::current_dir()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    let run_id = env::var("CAIRN_RUN_ID").ok();
    let mcp_secret = select_mcp_secret(
        env::var("CAIRN_MCP_SECRET").ok(),
        cairn_common::auth::load_local_mcp_token,
    );
    let home_uri = env::var("CAIRN_HOME_URI")
        .ok()
        .filter(|uri| parse_cairn_uri(uri).is_some());
    CairnCmd::new_with_home_uri(callback_url, cwd, run_id, mcp_secret, Vec::new(), home_uri)
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
async fn ensure_callback_reachable(callback_url: &str) -> bool {
    probe_callback(callback_url)
}

fn print_unreachable_callback(callback_url: &str) {
    eprintln!(
        "cairn: requires a running Cairn runner or server at {callback_url} (set CAIRN_CALLBACK_URL to override)."
    );
}

/// Parse a `ChangeInput` from a JSON object (with `changes`) or a bare array.
fn parse_change_input(raw: &str) -> Result<ChangeInput, String> {
    let value: serde_json::Value = serde_json::from_str(raw).map_err(|e| e.to_string())?;
    if value.is_array() {
        let changes: Vec<ChangeItemInput> =
            serde_json::from_value(value).map_err(|e| e.to_string())?;
        Ok(ChangeInput {
            changes: Some(changes),
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
    match client
        .read(Parameters(input), rmcp::model::RequestMetaObject::default())
        .await
    {
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
            cwd: client.cwd.to_string(),
            run_id: client.run_id.as_ref().map(|r| r.to_string()),
            tool: "watch".to_string(),
            payload,
            tool_use_id: None,
        };
        let outcome = client.call_tauri_full(&request).await;
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

fn check_run_request(client: &CairnCmd, suite: String, branch: Option<String>) -> CallbackRequest {
    let mut payload = serde_json::json!({ "suite": suite });
    if let Some(branch) = branch {
        payload["branch"] = serde_json::Value::String(branch);
    }
    CallbackRequest {
        thread_id: None,
        cwd: client.cwd.to_string(),
        run_id: client.run_id.as_ref().map(ToString::to_string),
        tool: "check_run".to_string(),
        payload,
        tool_use_id: None,
    }
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
        return Ok((format!("{headline}\n{cause}"), false));
    }

    let disposition = result
        .get("disposition")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| incomplete_check_result("the disposition"))?;
    let passed = result
        .get("passed")
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| incomplete_check_result("the verdict"))?;
    let observation = match result
        .get("observationId")
        .and_then(serde_json::Value::as_str)
    {
        Some(id) => format!("observation {id}"),
        None => "Cairn recorded no observation for this run".to_string(),
    };
    // Only a green is worth annotating: a red is never reusable evidence, and
    // saying so on every red would bury the one case that surprises a reader — a
    // pass another machine produced, which Cairn keeps but will not reuse.
    let reuse = match (
        passed,
        result
            .get("reusable")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        result
            .get("environmentFingerprint")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .is_empty(),
    ) {
        (true, false, true) => " · another machine ran this, so Cairn will not reuse the verdict",
        (true, false, false) => " · Cairn will not reuse this verdict",
        _ => "",
    };
    Ok((
        format!(
            "{} {suite} ({disposition}, {short_commit}) · {observation}{reuse}",
            if passed { "✓" } else { "✗" }
        ),
        passed,
    ))
}

pub(crate) async fn run_cli_check(suite: String, branch: Option<String>) -> bool {
    let callback_url = cli_callback_url();
    if !ensure_callback_reachable(&callback_url).await {
        print_unreachable_callback(&callback_url);
        return false;
    }
    let client = build_cli_client(callback_url);
    let outcome = client
        .call_tauri_full(&check_run_request(&client, suite, branch))
        .await;
    if !outcome.transport_ok {
        eprintln!("cairn check run: {}", outcome.result.trim_end());
        return false;
    }
    match render_check_run_response(&outcome.result) {
        Ok((summary, passed)) => {
            println!("{summary}");
            passed
        }
        Err(error) => {
            eprintln!("cairn check run: {error}");
            false
        }
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
    match client
        .write(Parameters(input), rmcp::model::RequestMetaObject::default())
        .await
    {
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
        let client = CairnCmd::new_with_home_uri(
            "http://localhost".into(),
            "/repo".into(),
            Some("run-1".into()),
            None,
            vec![],
            None,
        );
        let request = check_run_request(&client, "rust-tests".into(), None);
        assert_eq!(request.tool, "check_run");
        assert_eq!(
            request.payload,
            serde_json::json!({ "suite": "rust-tests" })
        );
        assert_eq!(request.run_id.as_deref(), Some("run-1"));
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
                    "reusable": true,
                    "environmentFingerprint": "env-local",
                    "observationId": "obs-source"
                }
            })
            .to_string(),
        )
        .unwrap();
        assert_eq!(
            rendered,
            (
                "✓ rust-tests (cached, 1234567890ab) · observation obs-source".to_string(),
                true,
            )
        );
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
                    "observationId": "obs-failed"
                }
            })
            .to_string(),
        )
        .unwrap();
        assert_eq!(
            summary,
            "✗ rust-tests (fresh, 1234567890ab) · observation obs-failed"
        );
        assert!(
            !passed,
            "a rendered red verdict must produce a nonzero CLI exit"
        );
    }

    /// The specimen this rendering exists for: a check that ran on another
    /// machine. Its verdict comes back like any other, keyed by an empty
    /// environment fingerprint, and the reply says plainly that Cairn will not
    /// reuse it.
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
                    "reusable": false,
                    "environmentFingerprint": "",
                    "observationId": "obs-remote"
                }
            })
            .to_string(),
        )
        .unwrap();
        assert!(passed, "a remote green is still a green");
        assert!(summary.starts_with("✓ rust-tests (fresh, 1234567890ab) · observation obs-remote"));
        assert!(
            summary.contains("another machine ran this"),
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
                    "reusable": false,
                    "environmentFingerprint": "",
                    "observationId": null
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
        let client = CairnCmd::new_with_home_uri(
            "http://localhost".into(),
            "/repo".into(),
            None,
            None,
            vec![],
            None,
        );
        let request = check_run_request(&client, "rust-tests".into(), Some("main@origin".into()));
        assert_eq!(
            request.payload,
            serde_json::json!({
                "suite": "rust-tests",
                "branch": "main@origin"
            })
        );
    }
}
