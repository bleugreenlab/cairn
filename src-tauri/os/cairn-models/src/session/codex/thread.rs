//! Opening a Codex thread on an app-server: resume, the MCP attachment check,
//! and native-goal reconciliation.

use crate::codex::app_server::AppServerClient;
use crate::interface::session::SessionStart;
use crate::session::mcp::McpServers;
use crate::wire::codex_app_server::goal::{
    parse_response, GoalClearResponse, GoalResponse, GoalStatus,
};

use super::params::is_missing_rollout_error;

pub fn verify_resumed_thread_binding(
    session_start: &SessionStart,
    thread_id: &str,
) -> Result<(), String> {
    if let SessionStart::Resume { backend_id, .. } = session_start {
        if backend_id != thread_id {
            return Err(format!(
                "Codex resume returned thread {thread_id}, but the Cairn session is durably bound to {backend_id}"
            ));
        }
    }
    Ok(())
}

pub fn reconcile_goal(
    client: &AppServerClient,
    thread_id: &str,
    expected_objective: Option<&str>,
    clear_native_goal: bool,
) -> Result<(), String> {
    if clear_native_goal {
        let response = client.send_request(
            "thread/goal/get",
            serde_json::json!({ "threadId": thread_id }),
        )?;
        if parse_response::<GoalResponse>("thread/goal/get", response)?
            .goal
            .is_some()
        {
            let response = client.send_request(
                "thread/goal/clear",
                serde_json::json!({ "threadId": thread_id }),
            )?;
            if !parse_response::<GoalClearResponse>("thread/goal/clear", response)?.cleared {
                return Err("Codex standing-thread goal could not be cleared".to_string());
            }
        }
        return Ok(());
    }
    let Some(expected_objective) = expected_objective else {
        return Ok(());
    };
    let response = client.send_request(
        "thread/goal/get",
        serde_json::json!({ "threadId": thread_id }),
    )?;
    let current = parse_response::<GoalResponse>("thread/goal/get", response)?.goal;
    if let Some(goal) = current {
        if goal.objective == expected_objective {
            return Ok(());
        }
        let params = serde_json::json!({
            "threadId": thread_id,
            "objective": expected_objective
        });
        let response = client.send_request("thread/goal/set", params)?;
        let reconciled = parse_response::<GoalResponse>("thread/goal/set", response)?
            .goal
            .ok_or_else(|| "Invalid Codex thread/goal/set response: missing goal".to_string())?;
        if reconciled.objective != expected_objective || reconciled.status != goal.status {
            return Err("Codex goal reconciliation returned unexpected state".to_string());
        }
        return Ok(());
    }

    let response = client.send_request(
        "thread/goal/set",
        serde_json::json!({
            "threadId": thread_id,
            "objective": expected_objective,
            "status": "active"
        }),
    )?;
    let goal = parse_response::<GoalResponse>("thread/goal/set", response)?
        .goal
        .ok_or_else(|| "Invalid Codex thread/goal/set response: missing goal".to_string())?;
    if goal.objective != expected_objective || goal.status != GoalStatus::Active {
        return Err("Codex goal reconciliation returned unexpected state".to_string());
    }
    Ok(())
}

pub fn resume_thread(
    client: &AppServerClient,
    backend_id: &str,
    resume_params: serde_json::Value,
    clear_native_goal: bool,
) -> Result<serde_json::Value, String> {
    if clear_native_goal {
        match reconcile_goal(client, backend_id, None, true) {
            Ok(()) => {}
            Err(error) if is_missing_rollout_error(&error, backend_id) => {
                // A stale rollout has no live provider goal to contain. Let the
                // ordinary resume path discover the stale id and rebuild from
                // Cairn's transcript.
            }
            Err(error) => return Err(error),
        }
    }
    client.send_request("thread/resume", resume_params)
}

/// Check every server in `servers` against the thread's MCP registry: each
/// must be present, and one with `required_tools` must advertise exactly them.
pub fn mcp_status(
    client: &AppServerClient,
    thread_id: &str,
    servers: &McpServers,
) -> Result<(), String> {
    let response = client.send_request(
        "mcpServerStatus/list",
        serde_json::json!({
            "threadId": thread_id,
            "detail": "toolsAndAuthOnly",
        }),
    )?;
    let registry = response
        .get("data")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "malformed mcpServerStatus/list response: missing data array".to_string())?;
    for server in servers {
        let name = server.name.as_str();
        let entry = registry
            .iter()
            .find(|entry| entry.get("name").and_then(serde_json::Value::as_str) == Some(name))
            .ok_or_else(|| format!("{name} server is absent from the thread MCP registry"))?;
        if server.required_tools.is_empty() {
            continue;
        }
        let tools = entry
            .get("tools")
            .and_then(serde_json::Value::as_object)
            .ok_or_else(|| format!("{name} server status is missing its tool inventory"))?;
        let mut advertised = tools.keys().map(String::as_str).collect::<Vec<_>>();
        advertised.sort_unstable();
        let mut required = server
            .required_tools
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        required.sort_unstable();
        if advertised != required {
            return Err(format!(
                "{name} server advertised tools [{}], required exactly [{}]",
                advertised.join(", "),
                required.join(", ")
            ));
        }
    }

    Ok(())
}

pub fn verify_resumed_thread_with_fallback<StartReplacement, Persist, ReportError>(
    client: &AppServerClient,
    servers: &McpServers,
    resumed_thread_id: &str,
    start_replacement: StartReplacement,
    mut persist: Persist,
    mut report_error: ReportError,
) -> Result<String, String>
where
    StartReplacement: FnOnce() -> Result<String, String>,
    Persist: FnMut(&str) -> Result<(), String>,
    ReportError: FnMut(&str),
{
    let result: Result<String, String> = (|| {
        if ensure_mcp_attached(client, resumed_thread_id, "resume", servers).is_ok() {
            persist(resumed_thread_id)?;
            return Ok(resumed_thread_id.to_string());
        }

        let replacement_thread_id = start_replacement()?;
        ensure_mcp_attached(
            client,
            &replacement_thread_id,
            "resume-transcript-fallback",
            servers,
        )
        .map_err(|reason| {
            format!(
                "replacement thread {replacement_thread_id} for resumed thread {resumed_thread_id} failed attachment: {reason}"
            )
        })?;
        persist(&replacement_thread_id)?;
        Ok(replacement_thread_id)
    })();
    if let Err(error) = &result {
        report_error(error);
    }
    result
}

/// Confirm the thread carries every server in `servers`, reloading the
/// app-server's MCP servers once and checking again before giving up.
pub fn ensure_mcp_attached(
    client: &AppServerClient,
    thread_id: &str,
    resume_mode: &str,
    servers: &McpServers,
) -> Result<(), String> {
    let names = servers.names().join(",");
    match mcp_status(client, thread_id, servers) {
        Ok(()) => {
            log::info!(
                "Codex MCP attachment verified: thread_id={} resume_mode={} server={} startup_state=ready",
                thread_id,
                resume_mode,
                names
            );
            return Ok(());
        }
        Err(first_reason) => log::warn!(
            "Codex MCP attachment unavailable; refreshing once: thread_id={} resume_mode={} server={} startup_state=unavailable reason={}",
            thread_id,
            resume_mode,
            names,
            first_reason
        ),
    }

    client
        .send_request("config/mcpServer/reload", serde_json::Value::Null)
        .map_err(|reason| format!("{names} MCP refresh failed for thread {thread_id}: {reason}"))?;
    mcp_status(client, thread_id, servers).map_err(|reason| {
        format!("{names} MCP attachment failed after refresh for thread {thread_id}: {reason}")
    })?;
    log::info!(
        "Codex MCP attachment recovered: thread_id={} resume_mode={} server={} startup_state=ready",
        thread_id,
        resume_mode,
        names
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::mcp::McpServer;
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    fn cairn() -> McpServers {
        McpServers::new(vec![McpServer::cairn_http("https://orch.example/mcp")]).unwrap()
    }

    fn ready_mcp_status() -> serde_json::Value {
        json!({
            "result": {
                "data": [{
                    "name": "cairn",
                    "tools": {
                        "read": {"name": "read"},
                        "write": {"name": "write"},
                        "run": {"name": "run"}
                    }
                }]
            }
        })
    }

    fn request_methods(requests: &Arc<Mutex<Vec<serde_json::Value>>>) -> Vec<String> {
        requests
            .lock()
            .unwrap()
            .iter()
            .filter_map(|request| request["method"].as_str().map(str::to_string))
            .collect()
    }

    fn goal(objective: &str, status: &str, token_budget: Option<u64>) -> serde_json::Value {
        json!({
            "threadId": "thread-1", "objective": objective, "status": status,
            "tokenBudget": token_budget, "tokensUsed": 7, "timeUsedSeconds": 8,
            "createdAt": 1, "updatedAt": 2
        })
    }

    #[test]
    fn reconciliation_refreshes_objective_without_activating_paused_goal() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let client = AppServerClient::for_test_scripted(
            vec![
                Ok(json!({"result": {"goal": goal("old projection", "paused", Some(500))}})),
                Ok(json!({"result": {"goal": goal("new projection", "paused", Some(500))}})),
            ],
            Arc::clone(&requests),
        );
        reconcile_goal(&client, "thread-1", Some("new projection"), false).unwrap();
        let requests = requests.lock().unwrap();
        assert_eq!(requests[1]["params"]["objective"], "new projection");
        assert!(requests[1]["params"].get("status").is_none());
        assert!(requests[1]["params"].get("tokenBudget").is_none());
    }

    #[test]
    fn reconciliation_preserves_paused_goal_with_current_objective() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let client = AppServerClient::for_test_scripted(
            vec![Ok(
                json!({"result": {"goal": goal("current", "paused", None)}}),
            )],
            Arc::clone(&requests),
        );
        reconcile_goal(&client, "thread-1", Some("current"), false).unwrap();
        assert_eq!(request_methods(&requests), vec!["thread/goal/get"]);
    }

    #[test]
    fn reconciliation_preserves_complete_goal() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let client = AppServerClient::for_test_scripted(
            vec![Ok(
                json!({"result": {"goal": goal("current", "complete", None)}}),
            )],
            Arc::clone(&requests),
        );
        reconcile_goal(&client, "thread-1", Some("current"), false).unwrap();
        assert_eq!(request_methods(&requests), vec!["thread/goal/get"]);
    }

    #[test]
    fn reconciliation_avoids_provider_write_when_objective_is_current() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let client = AppServerClient::for_test_scripted(
            vec![Ok(
                json!({"result": {"goal": goal("current", "active", None)}}),
            )],
            Arc::clone(&requests),
        );
        reconcile_goal(&client, "thread-1", Some("current"), false).unwrap();
        assert_eq!(request_methods(&requests), vec!["thread/goal/get"]);
    }

    #[test]
    fn resumed_thread_must_match_durable_session_binding() {
        let start = SessionStart::Resume {
            session_id: "session-1".to_string(),
            backend_id: "owned-thread".to_string(),
        };
        verify_resumed_thread_binding(&start, "owned-thread").unwrap();
        let error = verify_resumed_thread_binding(&start, "foreign-thread").unwrap_err();
        assert!(error.contains("durably bound to owned-thread"));
    }

    #[test]
    fn absent_objective_bypasses_native_goal_reconciliation() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let client = AppServerClient::for_test_scripted(Vec::new(), Arc::clone(&requests));

        reconcile_goal(&client, "nodeless-call", None, false).unwrap();

        assert!(request_methods(&requests).is_empty());
    }

    #[test]
    fn standing_thread_reconciliation_leaves_native_goal_absent() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let client = AppServerClient::for_test_scripted(
            vec![Ok(json!({"result": {"goal": null}}))],
            Arc::clone(&requests),
        );

        reconcile_goal(&client, "standing-thread", None, true).unwrap();

        assert_eq!(request_methods(&requests), ["thread/goal/get"]);
    }

    #[test]
    fn standing_thread_reconciliation_clears_a_legacy_native_goal() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let client = AppServerClient::for_test_scripted(
            vec![
                Ok(json!({"result": {"goal": {
                    "threadId": "standing-thread", "objective": "Tend forever",
                    "status": "active", "tokenBudget": null, "tokensUsed": 0,
                    "timeUsedSeconds": 0, "createdAt": 1, "updatedAt": 1
                }}})),
                Ok(json!({"result": {"cleared": true}})),
            ],
            Arc::clone(&requests),
        );

        reconcile_goal(&client, "standing-thread", None, true).unwrap();

        assert_eq!(
            request_methods(&requests),
            ["thread/goal/get", "thread/goal/clear"]
        );
    }

    #[test]
    fn standing_thread_goal_is_cleared_before_native_resume() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let client = AppServerClient::for_test_scripted(
            vec![
                Ok(json!({"result": {"goal": {
                    "threadId": "standing-thread", "objective": "Tend forever",
                    "status": "active", "tokenBudget": null, "tokensUsed": 0,
                    "timeUsedSeconds": 0, "createdAt": 1, "updatedAt": 1
                }}})),
                Ok(json!({"result": {"cleared": true}})),
                Ok(json!({"result": {"thread": {"id": "standing-thread"}}})),
            ],
            Arc::clone(&requests),
        );

        resume_thread(
            &client,
            "standing-thread",
            json!({"threadId": "standing-thread"}),
            true,
        )
        .unwrap();

        assert_eq!(
            request_methods(&requests),
            ["thread/goal/get", "thread/goal/clear", "thread/resume"]
        );
    }

    #[test]
    fn healthy_thread_verifies_without_refresh() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let client =
            AppServerClient::for_test_scripted(vec![Ok(ready_mcp_status())], Arc::clone(&requests));

        ensure_mcp_attached(&client, "thread-new", "new", &cairn()).unwrap();

        assert_eq!(request_methods(&requests), vec!["mcpServerStatus/list"]);
        assert_eq!(
            requests.lock().unwrap()[0]["params"]["threadId"],
            "thread-new"
        );
    }

    #[test]
    fn empty_resume_status_refreshes_then_recovers() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let client = AppServerClient::for_test_scripted(
            vec![
                Ok(json!({"result": {"data": []}})),
                Ok(json!({"result": {}})),
                Ok(ready_mcp_status()),
            ],
            Arc::clone(&requests),
        );

        ensure_mcp_attached(&client, "thread-resume", "resume", &cairn()).unwrap();

        assert_eq!(
            request_methods(&requests),
            vec![
                "mcpServerStatus/list",
                "config/mcpServer/reload",
                "mcpServerStatus/list"
            ]
        );
    }

    #[test]
    fn healthy_resume_checks_the_existing_thread_without_replacement() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let client =
            AppServerClient::for_test_scripted(vec![Ok(ready_mcp_status())], Arc::clone(&requests));

        ensure_mcp_attached(&client, "existing-thread", "resume", &cairn()).unwrap();

        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0]["params"]["threadId"], "existing-thread");
    }

    #[test]
    fn persistent_callback_failure_never_sends_turn_start() {
        let callback_error =
            json!({"error": {"code": -32000, "message": "callback connection refused"}});
        let requests = Arc::new(Mutex::new(Vec::new()));
        let client = AppServerClient::for_test_scripted(
            vec![
                Ok(callback_error.clone()),
                Ok(json!({"result": {}})),
                Ok(callback_error),
            ],
            Arc::clone(&requests),
        );

        let reported = Arc::new(Mutex::new(Vec::<String>::new()));
        let error = verify_resumed_thread_with_fallback(
            &client,
            &cairn(),
            "thread-offline",
            || Err("replacement app-server unavailable".to_string()),
            |_| Ok(()),
            {
                let reported = Arc::clone(&reported);
                move |error| reported.lock().unwrap().push(error.to_string())
            },
        )
        .unwrap_err();

        assert!(error.contains("replacement app-server unavailable"));
        assert_eq!(reported.lock().unwrap().as_slice(), [error.as_str()]);
        assert!(!request_methods(&requests).contains(&"turn/start".to_string()));
    }

    #[test]
    fn resumed_thread_fallback_persists_replacement_before_turn_start() {
        let empty_status = json!({"result": {"data": []}});
        let requests = Arc::new(Mutex::new(Vec::new()));
        let persisted = Arc::new(Mutex::new(Vec::<String>::new()));
        let errors = Arc::new(Mutex::new(Vec::<String>::new()));
        let client = AppServerClient::for_test_scripted(
            vec![
                Ok(empty_status.clone()),
                Ok(json!({"result": {}})),
                Ok(empty_status),
                Ok(json!({"result": {"thread": {"id": "replacement-thread"}}})),
                Ok(ready_mcp_status()),
                Ok(json!({"result": {"turn": {"id": "turn-1"}}})),
            ],
            Arc::clone(&requests),
        );

        let verified_thread = verify_resumed_thread_with_fallback(
            &client,
            &cairn(),
            "stale-thread",
            || {
                let replacement =
                    client.send_request("thread/start", json!({"cwd": "/tmp/worktree"}))?;
                replacement["thread"]["id"]
                    .as_str()
                    .map(str::to_string)
                    .ok_or_else(|| "replacement missing id".to_string())
            },
            {
                let persisted = Arc::clone(&persisted);
                move |thread_id| {
                    persisted.lock().unwrap().push(thread_id.to_string());
                    Ok(())
                }
            },
            {
                let errors = Arc::clone(&errors);
                move |error| errors.lock().unwrap().push(error.to_string())
            },
        )
        .unwrap();
        assert_eq!(verified_thread, "replacement-thread");
        assert_eq!(persisted.lock().unwrap().as_slice(), ["replacement-thread"]);
        assert!(errors.lock().unwrap().is_empty());

        client
            .send_request(
                "turn/start",
                json!({"threadId": verified_thread, "input": []}),
            )
            .unwrap();

        assert_eq!(
            request_methods(&requests),
            vec![
                "mcpServerStatus/list",
                "config/mcpServer/reload",
                "mcpServerStatus/list",
                "thread/start",
                "mcpServerStatus/list",
                "turn/start",
            ]
        );
    }

    #[test]
    fn missing_core_mcp_verb_is_rejected_after_one_refresh() {
        let status = json!({
            "result": {
                "data": [{
                    "name": "cairn",
                    "tools": {
                        "read": {"name": "read"},
                        "write": {"name": "write"}
                    }
                }]
            }
        });
        let requests = Arc::new(Mutex::new(Vec::new()));
        let client = AppServerClient::for_test_scripted(
            vec![Ok(status.clone()), Ok(json!({"result": {}})), Ok(status)],
            Arc::clone(&requests),
        );

        let error = ensure_mcp_attached(&client, "thread-broken", "resume", &cairn()).unwrap_err();

        assert!(error.contains("required exactly [read, run, write]"));
        assert_eq!(
            request_methods(&requests),
            vec![
                "mcpServerStatus/list",
                "config/mcpServer/reload",
                "mcpServerStatus/list"
            ]
        );
    }

    /// A host's own servers: every one must be present, and one with no
    /// required tools is checked for presence only.
    #[test]
    fn every_named_server_must_be_attached() {
        let servers = McpServers::new(vec![
            McpServer {
                name: "tm".to_string(),
                required_tools: Vec::new(),
                ..McpServer::cairn_http("http://x")
            },
            McpServer {
                name: "deck".to_string(),
                required_tools: vec!["get".to_string()],
                ..McpServer::cairn_http("http://y")
            },
        ])
        .unwrap();
        let tm_only = json!({"result": {"data": [{"name": "tm", "tools": {"anything": {}}}]}});
        let both = json!({"result": {"data": [
            {"name": "tm", "tools": {}},
            {"name": "deck", "tools": {"get": {}}}
        ]}});
        let requests = Arc::new(Mutex::new(Vec::new()));
        let client = AppServerClient::for_test_scripted(
            vec![Ok(tm_only.clone()), Ok(json!({"result": {}})), Ok(tm_only)],
            Arc::clone(&requests),
        );
        let error = ensure_mcp_attached(&client, "t", "new", &servers).unwrap_err();
        assert!(error.contains("deck server is absent"), "{error}");

        let client =
            AppServerClient::for_test_scripted(vec![Ok(both)], Arc::new(Mutex::new(Vec::new())));
        ensure_mcp_attached(&client, "t", "new", &servers).unwrap();
    }
}
