//! CLI subcommands (thin client: forward to the running app, print to stdout).
//! `read`/`write`/`watch` build a callback request, forward it over the same
//! HTTP callback the MCP server uses, and print the result.
use rmcp::handler::server::tool::Parameters;
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

/// Callback URL for CLI use: explicit env var (set by the runner for in-run
/// invocations), else the runner transport port.
fn cli_callback_url() -> String {
    env::var("CAIRN_CALLBACK_URL").unwrap_or_else(|_| default_callback_url())
}

/// Build a thin `CairnCmd` client from the environment for CLI forwarding.
fn build_cli_client(callback_url: String) -> CairnCmd {
    let cwd = env::current_dir()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    let run_id = env::var("CAIRN_RUN_ID").ok();
    let mcp_secret = env::var("CAIRN_MCP_SECRET")
        .ok()
        .or_else(cairn_common::auth::load_local_mcp_token);
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
        .read(Parameters(input), rmcp::model::Meta::default())
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
        .write(Parameters(input), rmcp::model::Meta::default())
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
}
